# 电商大数据数仓实战项目 (data-governance-demo)
> 纯自学搭的数仓全链路项目，从「离线数据同步+实时数据同步」到可视化一条龙，覆盖数仓分层、任务调度、数据校验、监控可视化全流程。

## 一、为啥做这个项目？
想自己搭个实战项目，从电商业务数据出发，把「采集（离线+实时）→加工→分析→可视化」全流程走一遍，入门实时数仓链路。

## 二、用到的核心技术栈
全是大数据岗高频用到的工具，纯实战配置，区分离线/实时能力：
- 数据存储：MySQL（模拟电商业务库）、Hive（数仓分层存数据）
- 数据同步：
  - 离线同步：Sqoop（MySQL↔Hive全量/增量同步，兜底用）
  - 实时同步：Canal+Kafka+Flink（捕获MySQL binlog，实时同步到ODS/DWD）
- 数据加工：
  - 离线加工：Hive SQL（核心清洗/聚合）、Shell/Python（写脚本自动化）、Spark（增量数据处理）
  - 实时加工：Flink SQL（实时清洗ODS数据到DWD）
- 数据校验：Python/Shell（防止数据丢了、加工乱了）
- 任务调度：Airflow（自动跑离线任务）
- 监控可视化：Prometheus+Grafana（看运行指标）、Python（Pandas+Matplotlib出业务图表，进阶可换BI工具）

## 三、数仓分层设计
按企业常用的三层来，职责清晰，同时适配离线/实时数据入仓：
1. **ODS层（操作数据层）**：数据仓库的「原材料库」
   - 离线：直接同步MySQL的原始数据，按日期分区存储，保证数据原汁原味
   - 实时：通过Canal捕获MySQL binlog，经Kafka+Flink实时写入ODS层，保留原始日志格式
2. **DWD层（数据明细层）**：数据「加工厂」
   - 离线：清洗ODS脏数据（去空格、改异常值、统一时间格式），生成干净的明细宽表
   - 实时：基于Flink SQL实时清洗ODS层CDC数据，同步生成DWD明细宽表（支持指定日期重跑）
3. **DWS层（数据服务层）**：数据「成品库」
   - 离线：按业务主题（用户、商品、订单）汇总核心指标（每日订单量、新增用户数）
   - （待完善）实时：基于DWD实时明细，分钟级汇总核心业务指标

## 四、覆盖的业务范围
初学不搞复杂的，聚焦电商核心3大主题+1个转化分析，同时支持离线/实时链路：
- 核心表：用户表（u_user）、商品表（p_product）、订单表（o_order）、订单明细表（o_order_detail）
- 数据链路：
  - 离线链路：MySQL业务库 → Sqoop → Hive ODS → Hive DWD → Hive DWS → 可视化图表
  - 实时链路：MySQL业务库 → Canal → Kafka → Flink → Hive ODS → Flink → Hive DWD → （待完善）DWS实时层

## 五、项目目录结构（按功能分）
```
data-governance-demo/
├── check/               # 数据校验：防同步/加工出问题
│   ├── 04_rowcheck_batch.py  # 校验MySQL和Hive行数（支持指定日期）
│   └── 05_partition_check.sh # 检查Hive分区有没有、有没有数据
├── dags/                # Airflow调度：自动跑全流程，不用手动盯
│   └── full_data_flow_dag.py  # 一键跑MySQL→ODS→DWD→DWS（离线版，待完善）
├── ddl/                 # 建表语句：所有库表结构都在这        
│   ├── hive/
│   │   ├── dwd_ddl.sql    # Hive DWD层建表
│   │   ├── dws_ddl.sql    # Hive DWS层建表
│   │   └── ods_ddl.sql    # Hive ODS层建表
│   └── mysql/
│       └── mysql_ddl.sql  # MySQL业务库（demo_oltp）建表
├── doc/                 # 说明文档
│   └── Prometheus_Readme.md   # Prometheus使用说明
├── logs/                # 执行日志：自动生成，方便排错（已忽略Git提交）
├── shell/               # 核心脚本：支持指定日期重跑（区分离线/实时）
│   ├── 00_gen_data.py   # 生成测试数据（增量同步测试用）
│   ├── 00_pre_data.sh   # 生成测试数据（全量同步测试用）
│   ├── 00_clean_all.sh  # 清理环境
│   ├── 03_sqoop_ods.sh  # MySQL→Hive ODS全量同步（离线）
│   ├── 03_sqoop_ods_bak.sh   # 无指标监控版（离线）
│   ├── 03_sqoop_ods_incremental.sh # MySQL→Hive ODS增量同步（离线）
│   ├── 06_run_dwd.sh    # ODS→DWD数据清洗（离线）
│   ├── 06_run_dwd_bak.sh    # 无指标监控版（离线）
│   ├── 07_run_dws.sh    # DWD→DWS指标汇总（离线）
│   ├── 07_run_dws_bak.sh    # 无指标监控版（离线）
│   ├── metrics_reporter.sh    # 监控指标上报脚本
│   ├── mysql_to_ods_dwd_dws.sh   # 全链路一键执行（离线定时调度用）
│   ├── mysql_to_ods_dwd_dws_bak.sh    # 无指标监控版（离线，未完善）
│   └── ods2dwd_cdc_sync.sh     # Flink ODS→DWD增量同步（实时，支持指定日期）
├── sql/                 # 业务逻辑：清洗/汇总SQL（区分离线/实时）
│    ├── flink/          # Flink实时同步SQL（核心）
│    │   ├── 00_ods_common_config.sql               # Flink CDC通用配置（优先执行）
│    │   ├── 01_ods_user_kafka_source.sql           # 用户表Kafka源表定义（CDC）
│    │   ├── 02_ods_product_kafka_source.sql        # 商品表Kafka源表定义（CDC）
│    │   ├── 03_ods_order_kafka_source.sql          # 订单表Kafka源表定义（CDC）
│    │   ├── 04_ods_order_detail_kafka_source.sql   # 订单明细表Kafka源表定义（CDC）
│    │   ├── 11_ods_user_cdc_insert.sql             # 用户表ODS CDC实时写入
│    │   ├── 12_ods_product_cdc_insert.sql          # 商品表ODS CDC实时写入
│    │   ├── 13_ods_order_cdc_insert.sql            # 订单表ODS CDC实时写入
│    │   ├── 14_ods_order_detail_cdc_insert.sql     # 订单明细表ODS CDC实时写入
│    │   ├── 21_dwd_user_cdc_incr_sync.sql          # 用户表DWD增量同步（实时）
│    │   ├── 22_dwd_product_cdc_incr_sync.sql       # 商品表DWD增量同步（实时）   
│    │   ├── 23_dwd_order_cdc_incr_sync.sql         # 订单表DWD增量同步（实时）
│    │   ├── 24_dwd_order_detail_cdc_sync.sql       # 订单明细表DWD增量同步（实时）
│    │   └── 99_validate_cdc_data.sql               # CDC数据验证（实时同步测试用）
│    ├── hive/           # Hive离线加工SQL
│    │   ├── dwd_*.sql      # DWD层清洗逻辑（去脏、标准化）
│    │   └── dws_*.sql      # DWS层指标汇总（订单/用户/商品/转化）
│    └── spark/          # Spark增量处理（学习用，未全部完善）
│        ├── 00_kafka_canal_temp.sql
│        ├── 11_canal_u_user_temp.sql
│        ├── 12_kafka_insert_to_ods_u_user.sql
│        ├── 21_canal_p_product_temp.sql
│        ├── 22_kafka_insert_to_ods_p_product.sql
│        ├── 31_canal_o_order_temp.sql
│        ├── 32_kafka_insert_to_ods_o_order.sql
│        ├── 41_canal_o_order_detail_temp.sql
│        ├── 42_kafka_insert_to_ods_o_order_detail.sql
│        └── spark数仓增量入仓核心流程.md   # Spark脚本使用说明
├── show/                # 可视化：DWS层出图
│   ├── csv_data/        # DWS层导出的CSV（Excel也能打开）
│   ├── py_analysis/     # Python可视化脚本
│   │   └── 00_all_analysis.py # 一键生成四大主题趋势图
│   ├── shell/           # DWS数据导出脚本
│   │   └── dws_export_all.sh  # 一键导出所有DWS表数据
│   └── README.md        # 可视化模块使用说明
├── .gitignore           # Git忽略规则：不提交日志、缓存等
└── README.md            # 项目说明（就是你现在看的这个）
```

## 六、快速上手（按步骤来，轻松跑通全流程）
### 前置准备
1. 基础环境：装好MySQL、Hive、Sqoop、Airflow（离线必备）
2. 实时环境：额外装好Canal、Kafka、Flink（推荐Flink 1.16+，适配Hive CDC）
3. 配置修改：
   - 改`shell/`脚本里的MySQL连接信息（IP、端口、账号密码）
   - 改`sql/flink/`下的Kafka/Canal/Flink配置（Kafka地址、Canal主题、Hive连接信息）

### 执行步骤
#### 方式1：跑离线全链路（原有逻辑，优先验证）
1. **初始化库表**：先把MySQL和Hive的表建好
   ```bash
   # MySQL建表
   mysql -u root -p < ddl/mysql/mysql_ddl.sql
   # Hive建表（按顺序执行）
   hive -f ddl/hive/ods_ddl.sql
   hive -f ddl/hive/dwd_ddl.sql
   hive -f ddl/hive/dws_ddl.sql
   ```
2. **生成测试数据**：往MySQL里插点测试数据
   ```bash
   sh shell/00_pre_data.sh
   ```
3. **全链路数据处理**：二选一
   - 一键搞定：`sh shell/mysql_to_ods_dwd_dws.sh`
   - 分步执行：
     ```bash
     sh shell/03_sqoop_ods.sh  # MySQL→ODS离线同步
     sh check/05_partition_check.sh  # 校验分区是否正常
     sh shell/06_run_dwd.sh    # ODS→DWD离线清洗
     sh shell/07_run_dws.sh    # DWD→DWS离线汇总
     ```
4. **可视化展示**：
   ```bash
   sh show/shell/dws_export_all.sh  # 导出DWS数据为CSV
   python show/py_analysis/00_all_analysis.py  # 生成业务趋势图
   ```

#### 方式2：跑实时CDC链路
1. **前置准备**：
   - 开启MySQL binlog（ROW模式），配置Canal监听MySQL业务库
   - 启动Kafka，创建Canal对应的主题（如canal_user、canal_product等）
   - 配置Flink集群，确保Flink能连接Hive和Kafka
2. **执行步骤**：
   - 查看路径下使用说明 sql/flink/README.md
3. **验证实时数据**：
   - 用`sql/flink/99_validate_cdc_data.sql`校验ODS/DWD实时数据
   - 往MySQL业务库手动插入测试数据，查看Hive ODS/DWD是否实时更新

## 七、离线&实时能力说明
### 离线部分（核心完善）
- Sqoop全量/增量同步：稳定兜底，适合T+1级别的数据处理
- Hive SQL加工：数仓核心逻辑，覆盖清洗、汇总全流程
- Airflow调度：支持定时重跑、失败重试，适配离线数仓调度需求

### 实时部分（CDC同步）
- Canal+Kafka：捕获MySQL binlog，解耦数据采集和消费
- Flink SQL：实时写入ODS、清洗到DWD，支持指定日期重跑补数
- 待完善：DWS层实时汇总、Flink任务监控、实时指标可视化

### Spark部分（学习用）
本来想搞「Flink实时+Spark增量+Sqoop全量兜底」的组合，暂先拿Spark学增量数据处理，后续可基于Spark完善离线增量逻辑～
