# Spark 数仓增量入仓核心流程（ODS层）- 可落地.md文档
## 一、整体流程逻辑（MySQL→Canal→Kafka→Spark SQL→Hive ODS）
基于已安装的Spark 3.3.6，实现**MySQL增量变更数据准实时同步到Hive ODS层**，作为数仓ODS层**Sqoop全量同步**的补充，形成「全量+增量」完整数据入仓体系，核心流程如下：
```
1. MySQL产生数据变更（INSERT/UPDATE/DELETE）→ 生成Binlog日志
2. Canal监听MySQL Binlog → 解析为JSON格式 → 发送到Kafka指定Topic（canal_demo_oltp）
3. Spark SQL通过Kafka连接器消费Topic数据 → 解析JSON为结构化数据
4. Spark SQL将解析后的增量数据 → 追加写入Hive ODS层分区表（适配dt按天分区）
```
### 核心定位
- 与Sqoop全量同步互补：Sqoop负责**历史全量/每日全量**数据入仓，Spark SQL负责**准实时增量**数据入仓
- 数仓链路起点：为后续DWD/DWS层数据清洗、聚合提供**完整、准实时**的ODS层基础数据

## 二、前置依赖（已完成）
### 1. 基础环境（已部署&验证）
- Hadoop 3.3.6（单/伪分布式，jps可见NameNode/DataNode）
- Hive 3.1.2（元数据正常，ODS层表已创建，库名：demo_ods）
- Kafka 3.6.1（服务正常运行，Topic：canal_demo_oltp已存在且有Canal增量数据）
- Canal（已配置监听MySQL，正常向Kafka推送JSON数据）
- JDK 1.8+（所有大数据组件通用依赖）

### 2. Spark 3.3.6 基础配置
#### 2.1 环境变量配置（/etc/profile）
```bash
export SPARK_HOME=/opt/bigdata/spark  -- 基于我的bigdata程序安装路径
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export SPARK_CONF_DIR=$SPARK_HOME/conf
# 使配置生效
source /etc/profile
```
#### 2.2 对接Hive/Hadoop核心配置（复制配置文件）
```bash
# 进入Spark配置目录
cd /opt/bigdata/spark/conf
# 复制Hive元数据配置（核心，Spark SQL识别Hive表）
cp /opt/bigdata/hive-3.1.2/conf/hive-site.xml ./
# 复制Hadoop配置（Spark依赖HDFS存储）
cp /opt/bigdata/hadoop/etc/hadoop/core-site.xml ./
cp /opt/bigdata/hadoop/etc/hadoop/hdfs-site.xml ./
```
#### 2.3 Spark核心参数配置（spark-defaults.conf）
```bash
# 复制模板并编辑  /opt/bigdata/spark/conf 路径下
cp spark-defaults.conf.template spark-defaults.conf
# 追加以下配置（单机环境适配，优化SQL执行）
spark.executor.memory 1g
spark.driver.memory 1g
spark.sql.adaptive.enabled true
spark.sql.catalogImplementation hive
spark.kryoserializer.buffer.max 128m
spark.streaming.kafka.consumer.cache.enabled false
```
#### 2.4 Spark-Kafka连接器依赖（下载到$SPARK_HOME/jars）
```bash
# 核心依赖包（共4个，缺一不可）
1. spark-sql-kafka-0-10_2.12-3.3.0.jar
2. kafka-clients-3.3.1.jar
3. spark-token-provider-kafka-0-10_2.12-3.3.0.jar
4. commons-pool2-2.11.1.jar
```

## 三、目录结构规划（基于我自己的服务器）
### 1. 项目SQL目录（data-governance-demo/sql/spark）
按原有表执行顺序 + 表标识组合编号命名脚本，与 hive 目录同级，便于调度、维护和表关联识别，结构规范且扩展性强，后续扩展表直接按规则追加即可：
```bash
[data-governance-demo]$ tree -L 2 sql
sql
├── hive/          # Hive DWD/DWS层脚本（原有结构不变）
└── spark/         # Spark ODS层增量入仓脚本（按「表标识+步骤」编号，00为全局前置）
    ├── 00_kafka_canal_temp.sql        # 全局前置步骤：创建Kafka数据源临时视图，所有表复用，仅需执行1次
    ├── 11_canal_u_user_temp.sql       # u_user表-步骤1：创建表专属解析视图（表标识1，步骤1）
    └── 12_kafka_insert_to_ods_u_user.sql  # u_user表-步骤2：增量写入ODS层u_user表（表标识1，步骤2）
# 后续扩展表按「表标识递增+步骤1/2」追加，示例：
# 21_canal_p_product_temp.sql、22_kafka_insert_to_ods_p_product.sql（p_product表，标识2）
# 31_canal_o_order_temp.sql、32_kafka_insert_to_ods_o_order.sql（o_order表，标识3）
# 41_canal_o_order_detail_temp.sql、42_kafka_insert_to_ods_o_order_detail.sql（o_order_detail表，标识4）
```

### 2. 编号与命名规则说明
(1) **脚本编号规则**
参照 Sqoop 原有执行顺序，不打乱现有逻辑，采用两位数字组合：
- 第一位：表唯一标识（从 1 开始递增，`1=u_user`、`2=p_product`、`3=o_order`、`4=o_order_detail`）；
- 第二位：表内执行步骤（`1 = 创建表专属解析视图`、`2 = 增量数据写入 ODS 表`）；
- `00` 开头：全局通用前置脚本，所有表操作的基础，无表关联，仅需执行 1 次。

(2) **执行顺序规则**
严格按“全局前置→单表两步”执行，多表执行时无需重复执行全局脚本：
```
00（全局前置）→ 11→12（u_user表）→21→22（p_product表）→31→32（o_order表）→41→42（o_order_detail表）
```

(3) **脚本命名规则**
后缀与功能强绑定，见名知意，无需查阅脚本内容即可识别用途：
- 解析视图脚本：`canal_<表名>_temp.sql`（对应步骤 1）；
- ODS 写入脚本：`kafka_insert_to_ods_<表名>.sql`（对应步骤 2）。

## 四、核心执行步骤（按顺序，配套脚本）
所有操作在**服务器终端**执行，全程基于`spark-sql`客户端，脚本存放在`data-governance-demo/sql/spark`目录，可**直接执行脚本**或**复制脚本内容到spark-sql客户端执行**。

### 前置操作：进入项目目录（方便执行脚本）
```bash
cd /home/donglin/data-governance-demo
```

### 步骤1：启动Spark SQL客户端（全局任意目录可执行）
```bash
spark-sql
# 成功标识：终端显示 spark-sql> 提示符
```

### 步骤2：执行00_kafka_canal_temp.sql（创建Kafka数据源临时视图）
#### 脚本路径：sql/spark/00_kafka_canal_temp.sql
#### 脚本内容：
```sql
-- 功能：创建Kafka数据源临时视图，消费Canal推送的增量数据
-- 注意：Spark批处理模式仅支持startingOffsets=earliest（消费全量历史+新数据）
CREATE TEMPORARY VIEW kafka_canal_temp
USING kafka
OPTIONS (
  "kafka.bootstrap.servers" = "127.0.0.1:9092",  # Kafka本机地址
  "subscribe" = "canal_demo_oltp",               # Canal推送的Topic
  "startingOffsets" = "earliest",                # 批处理模式必选值
  "includeHeaders" = "false"                     # 不读取Kafka消息头，仅读体
);
```
#### 执行方式（二选一）：
```bash
# 方式1：spark-sql客户端内直接执行脚本（推荐）
spark-sql> source sql/spark/00_kafka_canal_temp.sql;
# 方式2：终端直接执行脚本
spark-sql -f sql/spark/00_kafka_canal_temp.sql
# 成功标识：无报错，回到 spark-sql> 提示符
```

### 步骤3：执行11_canal_u_user_temp.sql（创建u_user表解析视图）
#### 关键说明
```
-- 数据类型强转：严格匹配 Hive ODS 表结构，如price/total_amount强转为DECIMAL(10,2)，order_status强转为TINYINT，所有 ID / 数量强转为INT；
-- JSON 解析字段：完全对齐 MySQL 表字段，确保data[0].字段名与 MySQL 表字段一致；
-- 过滤条件：精准过滤demo_oltp库对应表的数据，避免跨库 / 跨表脏数据；
-- 分区生成：统一使用op_ts / 1000将毫秒级时间戳转为秒级，再生成yyyy-MM-dd格式的 dt 分区，与 Sqoop 全量同步的分区规则一致。

```
#### 脚本路径：sql/spark/11_canal_u_user_temp.sql
#### 脚本内容：
```sql
-- 功能：解析Kafka的JSON数据，过滤u_user表，生成结构化临时视图
-- 核心：Kafka的value是Binary类型，需先CAST为STRING再解析JSON
CREATE TEMPORARY VIEW canal_u_user_temp
AS
SELECT
  -- 解析Canal JSON根节点字段
  get_json_object(CAST(value AS STRING), '$.database') AS database,  # 数据库名
  get_json_object(CAST(value AS STRING), '$.table') AS table_name,   # 表名
  get_json_object(CAST(value AS STRING), '$.type') AS op_type,       # 操作类型：INSERT/UPDATE/DELETE
  cast(get_json_object(CAST(value AS STRING), '$.ts') AS bigint) AS op_ts,  # 操作时间戳（毫秒级）
  -- 解析Canal JSON data数组（单条变更为数组[0]）
  get_json_object(CAST(value AS STRING), '$.data[0].user_id') AS user_id,
  get_json_object(CAST(value AS STRING), '$.data[0].username') AS username,
  get_json_object(CAST(value AS STRING), '$.data[0].register_time') AS register_time,
  get_json_object(CAST(value AS STRING), '$.data[0].update_time') AS update_time
FROM kafka_canal_temp
-- 精准过滤：仅demo_oltp库的u_user表数据
WHERE
  get_json_object(CAST(value AS STRING), '$.database') = 'demo_oltp'
  AND get_json_object(CAST(value AS STRING), '$.table') = 'u_user'
  AND get_json_object(CAST(value AS STRING), '$.data[0]') IS NOT NULL;  # 过滤无效数据

-- 可选：验证解析结果
-- SELECT * FROM canal_u_user_temp LIMIT 10;
```
#### 执行方式：
```sql
spark-sql> source sql/spark/11_canal_u_user_temp.sql;
# 成功标识：无报错，可执行SELECT * FROM canal_u_user_temp;验证数据
```

### 步骤4：执行12_kafka_insert_to_ods_u_user.sql（增量写入Hive ODS层）
#### 脚本路径：sql/spark/12_kafka_insert_to_ods_u_user.sql
#### 脚本内容：
```sql
-- 功能：将解析后的u_user增量数据，追加写入Hive ODS层分区表
-- 核心配置：关闭Hive动态分区严格模式（会话级，仅当前生效）
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 切换到ODS层库（避免表名前缀）
USE demo_ods;

-- 增量写入：INSERT INTO（追加模式，不覆盖历史数据）
INSERT INTO ods_u_user (user_id, username, register_time, update_time, dt)
SELECT
  CAST(user_id AS INT) AS user_id,  # 强转类型：String→INT（匹配Hive表）
  username,
  register_time,
  update_time,
  -- 毫秒级时间戳→dt分区（yyyy-MM-dd，数仓标准按天分区）
  FROM_UNIXTIME(CAST(op_ts / 1000 AS BIGINT), 'yyyy-MM-dd') AS dt
FROM canal_u_user_temp;

-- 验证步骤（执行完写入后，手动执行以下SQL验证）
-- 1. 查看创建的分区：SHOW PARTITIONS ods_u_user;
-- 2. 查看分区内数据：SELECT * FROM ods_u_user WHERE dt = '2026-02-01';（替换为实际日期）
```
#### 执行方式：
```sql
spark-sql> source sql/spark/12_kafka_insert_to_ods_u_user.sql;
# 成功标识：终端显示 Inserted 1 row(s)（记录数与解析结果一致）
```

### 步骤5：手动验证（可选，脚本内已注释，可直接执行）
```sql
-- 1. 查看ODS表分区（确认动态分区创建成功）
spark-sql> SHOW PARTITIONS demo_ods.ods_u_user;
-- 2. 查看增量数据（替换dt为实际操作日期）
spark-sql> SELECT * FROM demo_ods.ods_u_user WHERE dt = '2026-02-01';
-- 3. 关联全量数据（验证增量+全量无冲突）
spark-sql> SELECT username, update_time, dt FROM demo_ods.ods_u_user WHERE user_id = 1 ORDER BY dt DESC;
```

## 五、扩展其他表（p_product/o_order/order_detail）通用模板
基于u_user表脚本，**仅需修改表名、解析字段、写入字段**，核心逻辑完全复用，脚本按 ***编号规则说明*** 命名，步骤如下：
### 1. 新建解析视图脚本：x1_canal_<表名>_temp.sql
```sql
-- 复制11_canal_u_user_temp.sql，修改以下3处：
-- ① 视图名：canal_<表名>_temp
-- ② WHERE条件的表名：AND table = '<MySQL表名>'
-- ③ 解析字段：替换为当前表的字段（如product_id/ product_name/ price...）
CREATE TEMPORARY VIEW canal_p_product_temp
AS
SELECT
  get_json_object(CAST(value AS STRING), '$.database') AS database,
  get_json_object(CAST(value AS STRING), '$.table') AS table_name,
  get_json_object(CAST(value AS STRING), '$.type') AS op_type,
  cast(get_json_object(CAST(value AS STRING), '$.ts') AS bigint) AS op_ts,
  -- 替换为p_product表字段
  get_json_object(CAST(value AS STRING), '$.data[0].product_id') AS product_id,
  get_json_object(CAST(value AS STRING), '$.data[0].product_name') AS product_name,
  get_json_object(CAST(value AS STRING), '$.data[0].price') AS price,
  get_json_object(CAST(value AS STRING), '$.data[0].create_time') AS create_time,
  get_json_object(CAST(value AS STRING), '$.data[0].update_time') AS update_time
FROM kafka_canal_temp
WHERE
  get_json_object(CAST(value AS STRING), '$.database') = 'demo_oltp'
  AND get_json_object(CAST(value AS STRING), '$.table') = 'p_product'  # 替换为MySQL表名
  AND get_json_object(CAST(value AS STRING), '$.data[0]') IS NOT NULL;
```
### 2. 新建写入ODS脚本：x2_kafka_insert_to_ods_<表名>.sql
```sql
-- 复制12_kafka_insert_to_ods_u_user.sql，修改以下3处：
-- ① 写入表名：ods_<表名>
-- ② 字段列表：替换为当前表的字段（含dt）
-- ③ SELECT子句：替换为当前表的解析字段，注意类型强转
SET hive.exec.dynamic.partition.mode=nonstrict;
USE demo_ods;

INSERT INTO ods_p_product (product_id, product_name, price, create_time, update_time, dt)
SELECT
  CAST(product_id AS INT) AS product_id,
  product_name,
  CAST(price AS DECIMAL(10,2)) AS price,  # 注意Decimal类型强转
  create_time,
  update_time,
  FROM_UNIXTIME(CAST(op_ts / 1000 AS BIGINT), 'yyyy-MM-dd') AS dt
FROM canal_p_product_temp;  # 替换为当前表的解析视图名
```
### 3. 执行顺序（与u_user表一致）
```sql
spark-sql> source sql/spark/00_kafka_canal_temp.sql;  # 仅需执行1次，多表复用，前面执行了就不需要执行
spark-sql> source sql/spark/21_canal_p_product_temp.sql;
spark-sql> source sql/spark/22_kafka_insert_to_ods_p_product.sql;
```

## 六、关键注意事项（避坑指南）
### 1. Spark临时视图生命周期
- `TEMPORARY VIEW`是**会话级**，仅在当前spark-sql客户端生效，关闭/重启客户端后需重新执行脚本创建
- 多表同步时，`00_kafka_canal_temp.sql`仅需执行1次，所有表的解析视图均可复用该视图

### 2. 数据类型匹配（核心）
- Kafka解析的所有字段默认是**String**类型，写入Hive时需强转为对应类型：
  - INT类型：`CAST(user_id AS INT)`
  - DECIMAL类型：`CAST(price AS DECIMAL(10,2))`
  - TINYINT类型：`CAST(order_status AS TINYINT)`
- 不匹配会导致**写入失败**，严格按Hive ODS表结构强转。

### 3. Canal JSON格式固定规则
- `data`字段永远是**数组格式**，即使单条数据变更，也需用`data[0].字段名`解析
- `ts`字段是**毫秒级时间戳**，转换为dt分区时必须`/1000`转为秒级：`op_ts / 1000`
- 根节点字段：`database`（库名）、`table`（表名）、`type`（操作类型）、`ts`（时间戳）固定不变。

### 4. Hive动态分区配置
- 必须执行`SET hive.exec.dynamic.partition.mode=nonstrict;`，否则全动态分区（dt由SQL计算）写入失败
- 该配置**仅会话级生效**，每次启动spark-sql客户端后，执行写入脚本前需先执行该配置。

### 5. 写入方式（增量核心）
- 必须使用**INSERT INTO**（追加模式），禁止使用`INSERT OVERWRITE`（覆盖模式）
- `INSERT OVERWRITE`会覆盖分区内所有数据（包括Sqoop全量数据），导致数据丢失。

### 6. 服务保活
- 同步前确保**Kafka/Canal/MySQL**服务正常运行：
  - 验证Kafka：`ps -ef | grep kafka` + `netstat -tlnp | grep 9092`
  - 验证Canal：查看Canal日志，确认无报错且正常推送数据到Kafka
  - 服务异常会导致**消费失败**，无数据写入。

## 七、与Sqoop全量同步的配合方案
### 1. 执行时机调度（如用Azkaban/airflow）
```bash
# 每日全量同步（Sqoop+shell脚本）：凌晨2点执行
# 增量同步（Spark SQL脚本）：每30分钟/1小时执行一次
# 示例Azkaban调度流程：
1. 02:00：执行03_sqoop_ods.sh → 同步MySQL前一天全量数据到ODS层（dt=前一天）
2. 02:30 - 23:30：每30分钟执行一次Spark SQL脚本 → 同步增量数据到对应dt分区
```
### 2. 数据一致性保障
- 全量与增量的**dt分区规则一致**（均为yyyy-MM-dd）
- 增量数据的dt由**操作时间**决定，全量数据的dt由**同步日期**决定，天然隔离，无冲突。

## 八、脚本维护与扩展规范
### 1. 脚本编号
- 按**执行顺序**连续编号，新增表时按最大编号+1，不打乱原有顺序
- 示例：u_user（00/11/12）→ p_product（21/22）→ o_order（31/32）→ o_order_detail（41/42）

### 2. 脚本注释
- 每个脚本开头添加**功能说明**，关键行添加注释（如类型强转、时间戳转换）
- 便于后续维护和团队协作，示例参考现有脚本的注释风格。

### 3. 分区验证
- 每个写入脚本末尾添加**验证SQL**（注释形式），执行完写入后手动验证，确保数据正确。

### 4. 目录统一
- 所有Spark SQL脚本**统一存放在data-governance-demo/sql/spark**目录，与Hive脚本同级，便于项目管理和调度。
