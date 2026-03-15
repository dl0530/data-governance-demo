-- 商品表 Kafka 源表

DROP TABLE IF EXISTS kafka_canal_source_product;
CREATE TABLE kafka_canal_source_product (
    database STRING,
    `table` STRING,
    type STRING,
    data ARRAY<ROW(
        product_id INT,
        product_name STRING,
        price DECIMAL(10,2),
        create_time STRING,
        update_time STRING
    )>,
    ts BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'canal_demo_oltp',
    'properties.bootstrap.servers' = '127.0.0.1:9092',
    'properties.group.id' = 'flink_canal_cdc_product',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
