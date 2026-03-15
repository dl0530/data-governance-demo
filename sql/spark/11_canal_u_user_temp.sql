-- 解析Canal JSON数据，过滤u_user表，创建结构化临时视图
CREATE TEMPORARY VIEW canal_u_user_temp
AS
SELECT
  -- 先将Binary类型的value强转为String，再解析JSON
  get_json_object(CAST(value AS STRING), '$.database') AS database,  -- 数据库名
  get_json_object(CAST(value AS STRING), '$.table') AS table_name,   -- 表名
  get_json_object(CAST(value AS STRING), '$.type') AS op_type,       -- 操作类型：INSERT/UPDATE/DELETE
  cast(get_json_object(CAST(value AS STRING), '$.ts') AS bigint) AS op_ts,  -- 操作时间戳（毫秒级）
  -- 解析JSON data数组的第一个元素（单条数据变更，data是数组格式）
  get_json_object(CAST(value AS STRING), '$.data[0].user_id') AS user_id,
  get_json_object(CAST(value AS STRING), '$.data[0].username') AS username,
  get_json_object(CAST(value AS STRING), '$.data[0].register_time') AS register_time,
  get_json_object(CAST(value AS STRING), '$.data[0].update_time') AS update_time
-- 从已创建的Kafka临时视图读取数据
FROM kafka_canal_temp
-- 只过滤demo_oltp库的u_user表数据，精准匹配
WHERE
  get_json_object(CAST(value AS STRING), '$.database') = 'demo_oltp'
  AND get_json_object(CAST(value AS STRING), '$.table') = 'u_user'
  -- 过滤无效数据（确保data字段非空）
  AND get_json_object(CAST(value AS STRING), '$.data[0]') IS NOT NULL;

-- 关键解析说明
-- Canal 的 JSON 数据中，data是数组格式（即使单条数据变更也是数组），故用data[0]取第一条；
-- 保留op_type（操作类型）、op_ts（操作时间戳），用于 ODS 层增量标识，方便后续数仓分层处理；
-- 字段名与 MySQLu_user完全一致，确保能直接映射 Hive ODS 表。
