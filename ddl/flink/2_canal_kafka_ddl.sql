-- 功能：创建Flink Kafka源表，消费Canal同步的MySQL binlog数据
CREATE TABLE IF NOT EXISTS kafka_canal_demo_oltp (
    -- Canal输出的变更数据数组（通用MAP适配所有表字段）
    data ARRAY<MAP<STRING, STRING>>,
    -- 源数据库名（固定为demo_oltp）
    database STRING,
    -- 源表名（如u_user、p_product等）
    `table` STRING,
    -- 操作类型：INSERT/UPDATE/DELETE
    `type` STRING,
    -- Canal采集时间戳（毫秒级）
    ts BIGINT,
    -- 转换为带时区的时间戳（用于水位线）
    ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
    -- 水位线：允许5秒乱序延迟
    WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'canal_demo_oltp',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink_canal_demo_oltp',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
