-- 订单明细表 Kafka 源表

DROP TABLE IF EXISTS kafka_canal_source_order_detail;
CREATE TABLE kafka_canal_source_order_detail (
    database STRING,
    `table` STRING,
    type STRING,
    data ARRAY<ROW(
        detail_id INT,
        order_id INT,
        product_id INT,
        quantity INT,
        create_time STRING,
        update_time STRING
    )>,
    ts BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'canal_demo_oltp',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink_canal_cdc_order_detail',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
