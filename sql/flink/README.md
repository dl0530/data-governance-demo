### sql/flink/ 目录使用说明
#### 一、目录定位
该目录下为**电商实时CDC同步核心SQL**，基于Canal+Kafka+Flink实现MySQL变更数据（INSERT/UPDATE/DELETE）实时同步至Hive ODS/DWD层，支持手动分步执行（学习/调试）、Shell脚本自动化执行（生产）。

#### 二、文件分类&执行顺序
| 序号前缀 | 类型         | 文件名                          | 核心作用                                  | 执行优先级 |
|----------|--------------|---------------------------------|-------------------------------------------|------------|
| 00       | 通用配置     | 00_ods_common_config.sql        | 注册Hive Catalog、设置Flink基础参数       | 1（最先执行） |
| 01-04    | Kafka源表    | 01_ods_user_kafka_source.sql等  | 定义各业务表（用户/商品/订单/明细）Kafka源表（对接Canal） | 2 |
| 11-14    | ODS写入      | 11_ods_user_cdc_insert.sql等    | 将Kafka CDC数据实时写入Hive ODS层（保留全量变更） | 3 |
| 21-24    | DWD增量同步  | 21_dwd_user_cdc_incr_sync.sql等 | ODS层数据清洗合并至DWD层（保留最新数据）| 4 |
| 99       | 数据验证     | 99_validate_cdc_data.sql        | 校验CDC同步结果（ODS/DWD数据一致性）| 最后执行 |

#### 三、使用方式
##### 方式1：手动分步执行（学习/调试）
1. 进入Flink SQL Client：`/opt/bigdata/flink/bin/sql-client.sh embedded`
2. 按优先级依次执行SQL文件：
   ```bash
   # 1. 执行通用配置
   flink sql -f 00_ods_common_config.sql
   # 2. 执行Kafka源表定义（按需选表，如用户表）
   flink sql -f 01_ods_user_kafka_source.sql
   # 3. 执行ODS实时写入（如用户表）
   flink sql -f 11_ods_user_cdc_insert.sql
   # 4. （Hive CLI执行）DWD增量同步（替换${SYNC_DATE}为实际日期）
   hive -f 21_dwd_user_cdc_incr_sync.sql
   # 5. 验证同步结果
   hive -f 99_validate_cdc_data.sql
   ```

##### 方式2：自动化执行（生产）
无需手动执行21-24号文件，通过Shell脚本动态传参执行：
```bash
# 同步指定日期（如2026-03-15）
sh /opt/donglin/data-governance-demo/shell/ods2dwd_cdc_sync.sh 2026-03-15
# 默认同步前一天（生产定时任务用）
sh /opt/donglin/data-governance-demo/shell/ods2dwd_cdc_sync.sh
```

#### 四、注意事项
1. 执行前需确保：MySQL binlog开启（ROW模式）、Canal/Kafka/Flink/Hive网络互通；
2. 01-04/11-14号文件需在Flink SQL Client执行，21-24/99号文件在Hive CLI执行；
3. 21-24号文件中`${SYNC_DATE}`为日期占位符，手动执行需替换为实际日期；
4. 若同步失败，优先检查Kafka主题、Canal配置、Flink-Hive Catalog连接。
