-- 订单表 Kafka 源表

DROP TABLE IF EXISTS kafka_canal_source_order;
CREATE TABLE kafka_canal_source_order (
    database STRING,
    `table` STRING,
    type STRING,
    data ARRAY<ROW(
        order_id INT,
        user_id INT,
        total_amount DECIMAL(10,2),
        order_status TINYINT,
        create_time STRING,
        update_time STRING
    )>,
    ts BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'canal_demo_oltp',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink_canal_cdc_order',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
