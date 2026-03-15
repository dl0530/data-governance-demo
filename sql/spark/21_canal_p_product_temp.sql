-- 功能：解析Kafka的JSON数据，过滤p_product表，生成结构化临时视图
-- 核心：Kafka的value是Binary类型，需先CAST为STRING再解析JSON
CREATE TEMPORARY VIEW canal_p_product_temp
AS
SELECT
  -- 解析Canal JSON根节点字段
  get_json_object(CAST(value AS STRING), '$.database') AS database,  # 数据库名
  get_json_object(CAST(value AS STRING), '$.table') AS table_name,   # 表名
  get_json_object(CAST(value AS STRING), '$.type') AS op_type,       # 操作类型：INSERT/UPDATE/DELETE
  cast(get_json_object(CAST(value AS STRING), '$.ts') AS bigint) AS op_ts,  # 操作时间戳（毫秒级）
  -- 解析Canal JSON data数组（单条变更为数组[0]）
  get_json_object(CAST(value AS STRING), '$.data[0].product_id') AS product_id,
  get_json_object(CAST(value AS STRING), '$.data[0].product_name') AS product_name,
  get_json_object(CAST(value AS STRING), '$.data[0].price') AS price,
  get_json_object(CAST(value AS STRING), '$.data[0].create_time') AS create_time,
  get_json_object(CAST(value AS STRING), '$.data[0].update_time') AS update_time
FROM kafka_canal_temp
-- 精准过滤：仅demo_oltp库的p_product表数据
WHERE
  get_json_object(CAST(value AS STRING), '$.database') = 'demo_oltp'
  AND get_json_object(CAST(value AS STRING), '$.table') = 'p_product'
  AND get_json_object(CAST(value AS STRING), '$.data[0]') IS NOT NULL;  # 过滤无效数据

-- 可选：验证解析结果
-- SELECT * FROM canal_p_product_temp LIMIT 10;
