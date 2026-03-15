-- 增量写入ods_u_user表，按操作时间分区（dt=yyyy-MM-dd）
INSERT INTO ods_u_user (user_id, username, register_time, update_time, dt)
SELECT
  -- 强转类型匹配Hive表（视图中是字符串，Hive是INT）
  CAST(user_id AS INT) AS user_id,
  username,
  register_time,
  -- 更新操作取最新update_time，确保数据最新
  update_time,
  -- 核心：将Canal毫秒级时间戳转换为dt分区格式（yyyy-MM-dd）
  FROM_UNIXTIME(CAST(op_ts / 1000 AS BIGINT), 'yyyy-MM-dd') AS dt
FROM canal_u_user_temp;
