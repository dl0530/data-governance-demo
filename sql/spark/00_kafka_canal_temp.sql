-- Spark SQL执行，创建Kafka数据源临时视图，消费canal_demo_oltp Topic
-- Spark 会自动建立 Kafka 连接，无需额外配置依赖
CREATE TEMPORARY VIEW kafka_canal_temp
USING kafka
OPTIONS (
  -- Kafka集群地址（本机单机，与之前配置一致）
  "kafka.bootstrap.servers" = "127.0.0.1:9092",
  -- 目标Topic
  "subscribe" = "canal_demo_oltp",
  -- 消费起始位置：latest（仅消费新数据），消费历史数据为earliest
  "startingOffsets" = "earliest",
  -- 仅读取Kafka消息体（Canal的JSON数据在value中）
  "includeHeaders" = "false"
);
