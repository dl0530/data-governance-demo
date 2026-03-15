-- 注册 Hive Catalog 固定配置
CREATE CATALOG hive_catalog WITH (
    'type' = 'hive',
    'default-database' = 'demo_ods',
    'hive-conf-dir' = '/opt/bigdata/hive-3.1.2/conf'
);

-- 切换至 Hive Catalog 及目标库
USE CATALOG hive_catalog;

-- Hive DWD 层
USE demo_ods;

-- 设置默认并行度（根据集群资源调整）
SET parallelism.default = 1;

-- 通用参数：开启动态分区、实时提交分区、同步 Hive 元数据
SET hive.exec.dynamic.partition = 'true';
SET hive.exec.dynamic.partition.mode = 'nonstrict';
SET sink.partition-commit.trigger = 'process-time';
SET sink.partition-commit.delay = '0 s';
SET sink.partition-commit.policy.kind = 'success-file,metastore';

