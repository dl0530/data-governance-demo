prometheus-ecosystem/          # 【核心总目录】Prometheus 生态所有组件的根目录
├── logs/                      # 【统一日志目录】所有监控组件的日志存储（核心规范目录）
│   ├── node_exporter/         # Node Exporter 专属日志子目录（规范层级）
│   │   └── node_exporter.log  # Node Exporter 运行日志（启动日志、错误日志等）
│   └── prometheus/            # Prometheus 专属日志子目录（规范层级）
│       └── prometheus.log     # Prometheus 运行日志（启动日志、指标抓取日志、错误日志等）
├── node_exporter/             # Node Exporter 程序目录（核心组件：系统/自定义指标采集器）
│   ├── LICENSE                # 开源许可证文件（MIT 协议）
│   ├── node_exporter          # Node Exporter 可执行程序（核心二进制文件）
│   ├── NOTICE                 # 开源声明文件（版权、依赖说明）
│   └── textfile_collector/    # 自定义指标目录（核心！存储数仓同步的埋点指标）
│       └── data_warehouse_metrics.prom  # 数仓监控指标文件（Sqoop 同步成功率、耗时、行数等）
└── prometheus/                # Prometheus 程序目录（核心组件：指标存储/查询/告警）
    ├── data/                  # 【规范目录】Prometheus TSDB 数据目录（存储采集到的所有监控指标）
    │   ├── chunks_head/       # 内存活跃数据块（未刷盘的临时指标数据）
    │   ├── lock               # 数据目录锁文件（防止多进程读写冲突）
    │   ├── queries.active     # 活跃查询记录（当前正在执行的 PromQL 查询）
    │   └── wal/               # 预写日志（保障数据不丢失，崩溃后恢复）
    ├── LICENSE                # 开源许可证文件（Apache 2.0 协议）
    ├── NOTICE                 # 开源声明文件
    ├── prometheus             # Prometheus 可执行程序（核心二进制文件）
    ├── prometheus.yml         # Prometheus 核心配置文件（配置指标抓取目标、存储时长等）
    └── promtool               # Prometheus 工具程序（校验配置文件、导出数据、查错等）



 # 数据治理全流程脚本+监控指标说明（含查询语句）
以下是 `03_sqoop_ods.sh`/`06_run_dwd.sh`/`07_run_dws.sh`/`mysql_to_ods_dwd_dws.sh` 中所有监控指标的**含义+精准查询语句**，按“脚本-指标-用途”分类整理，可直接复制到Prometheus/Grafana中查询：

## 一、核心脚本功能说明
| 脚本文件               | 核心功能                                  | 监控重点                          |
|------------------------|-------------------------------------------|-----------------------------------|
| 03_sqoop_ods.sh        | ODS层全量同步（MySQL→Hive）| 同步行数、成功率、耗时            |
| 06_run_dwd.sh          | DWD层数据清洗（去重/标准化/过滤脏数据）| 步骤执行状态（成功/失败）|
| 07_run_dws.sh          | DWS层业务指标汇总计算                     | 步骤执行状态（成功/失败）|
| mysql_to_ods_dwd_dws.sh| 全流程总控（调用上述脚本+统一上报监控）| 全流程耗时、各步骤状态、全局指标  |

## 二、所有监控指标详解（含查询语句）
### 1. ODS层同步核心指标（来自03_sqoop_ods.sh+总控脚本）
| 指标名                                  | 指标含义                                  | 单位   | 精准查询语句（直接复制）|
|-----------------------------------------|-------------------------------------------|--------|--------------------------------------------------------------------------|
| data_warehouse_sync_rows                | 单表同步行数（如u_user/o_order表）| 行     | `data_warehouse_sync_rows{business="sqoop_ods_full_sync_u_user"}`         |
| data_warehouse_sync_total_rows          | ODS层所有表同步总行数                     | 行     | `data_warehouse_sync_total_rows{business="sqoop_ods_full_sync"}`          |
| data_warehouse_success_rate             | ODS层同步成功率（1=成功，0=失败）| 无     | `data_warehouse_success_rate{business="sqoop_ods_full_sync"}`             |
| data_warehouse_cost_time_seconds        | ODS层同步总耗时                           | 秒     | `data_warehouse_cost_time_seconds{business="sqoop_ods_full_sync"}`        |

### 2. 全流程监控指标（来自mysql_to_ods_dwd_dws.sh）
| 指标名                                  | 指标含义                                  | 单位   | 精准查询语句（直接复制）|
|-----------------------------------------|-------------------------------------------|--------|--------------------------------------------------------------------------|
| data_warehouse_full_flow_cost_time      | 数据治理全流程总耗时（ODS→DWD→DWS）| 秒     | `data_warehouse_full_flow_cost_time{business="full_data_governance_flow"}`|
| data_warehouse_full_flow_success        | 全流程执行结果（1=成功，0=失败）| 无     | `data_warehouse_full_flow_success{business="full_data_governance_flow"}`  |

### 3. 各步骤执行状态指标（覆盖06/07脚本）
| 指标名                                  | 指标含义                                  | 取值   | 精准查询语句（直接复制）|
|-----------------------------------------|-------------------------------------------|--------|--------------------------------------------------------------------------|
| data_warehouse_step_status              | ODS层同步步骤状态                        | 1=成功<br>0=失败 | `data_warehouse_step_status{business="full_data_governance_flow_ods_sync"}` |
| data_warehouse_step_status              | ODS表行数校验步骤状态                    | 1=成功<br>0=失败 | `data_warehouse_step_status{business="full_data_governance_flow_row_check"}` |
| data_warehouse_step_status              | ODS表分区校验步骤状态                    | 1=成功<br>0=失败 | `data_warehouse_step_status{business="full_data_governance_flow_partition_check"}` |
| data_warehouse_step_status              | DWD层清洗步骤状态                        | 1=成功<br>0=失败 | `data_warehouse_step_status{business="full_data_governance_flow_dwd_clean"}` |
| data_warehouse_step_status              | DWS层聚合步骤状态                        | 1=成功<br>0=失败 | `data_warehouse_step_status{business="full_data_governance_flow_dws_agg"}` |

## 三、常用查询语句
### 1. 查询某一天的所有指标（如2026-01-25）
```promql
{date="2026-01-25"}  # 匹配该日期的所有指标
```

### 2. 查询所有步骤的执行状态（快速排查失败步骤）
```promql
data_warehouse_step_status{business=~"full_data_governance_flow.*"}
```

### 3. 查询所有同步行数指标（对比各表数据量）
```promql
data_warehouse_sync_rows{business=~"sqoop_ods_full_sync.*"}
```

### 4. 查询全流程核心指标（总耗时+成功率）
```promql
data_warehouse_full_flow_cost_time OR data_warehouse_full_flow_success
```

## 四、关键说明
1. **标签含义**：
   - `business`：区分业务场景（如`sqoop_ods_full_sync`=ODS同步，`full_data_governance_flow`=全流程）；
   - `date`：指标对应的业务日期（如`date="2026-01-25"`），可加该标签精准过滤某一天的数据；
2. **取值规则**：
   - 状态类指标（如`step_status`/`success`）：1=成功，0=失败；
   - 耗时类指标：单位为秒，数值越大表示执行时间越长；
   - 行数类指标：仅反映同步的实际数据量，无负数；
3. **异常排查**：
   - 若`success_rate=0`：检查ODS同步脚本`03_sqoop_ods.sh`的执行日志；
   - 若`step_status=0`：定位对应步骤（如`dwd_clean`），查看该步骤的日志文件；
   - 若`sync_rows=0`：确认Hive表`dt=业务日期`是否有数据。

## 总结
1. 核心监控维度：**数据量（行数）、执行状态（成功/失败）、耗时（秒）、成功率**；
2. 精准查询：所有语句可直接复制到Prometheus/Grafana，加`date`标签可过滤指定日期；
3. 异常定位：通过`step_status`可快速找到全流程中失败的步骤，通过`sync_rows`可验证数据同步是否完整。
