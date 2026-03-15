-- 创建 Kafka 源表（按业务表拆分）
DROP TABLE IF EXISTS kafka_canal_source_user;
CREATE TABLE kafka_canal_source_user (
    database STRING,        -- Canal 采集的数据库名
    `table` STRING,         -- Canal 采集的表名
    type STRING,            -- 操作类型：INSERT/UPDATE/DELETE
    data ARRAY<ROW(         -- 业务字段（与 MySQL 表一致）
        user_id INT,
        username STRING,
        register_time STRING,
        update_time STRING
    )>,
    ts BIGINT               -- Canal 采集时间戳（毫秒）
) WITH (
    'connector' = 'kafka',
    'topic' = 'canal_demo_oltp',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink_canal_cdc_user',  -- 独立消费组
    'scan.startup.mode' = 'earliest-offset',        -- 首次启动从头消费
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',         -- 忽略缺失字段
    'json.ignore-parse-errors' = 'true'             -- 忽略 JSON 解析错误
);
