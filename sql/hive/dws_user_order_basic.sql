-- 跨主题（用户+订单）DWS层统计脚本
-- 功能：关联用户和订单表，计算下单转化指标
-- 执行方式：
--   1. 默认昨天：hive -f sql/dws_user_order_basic.sql
--   2. 指定日期：hive -hivevar dt=2025-11-01 -f sql/dws_user_order_basic.sql
-- 依赖表：demo_dwd.dwd_u_user、demo_dwd.dwd_o_order
-- 输出表：demo_dws.dws_user_order_basic_di（按dt分区）

-- 定义日期变量（默认昨天）
SET dt=${dt:-$(date -d "yesterday" +%Y-%m-%d)};

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 子查询：当日活跃用户（订单用户+新注册用户）
WITH daily_active_users AS (
  SELECT DISTINCT user_id FROM demo_dwd.dwd_o_order WHERE dt = '${dt}'
  UNION
  SELECT DISTINCT user_id FROM demo_dwd.dwd_u_user 
   WHERE dt = '${dt}' AND date(register_time) = date('${dt}')
)

INSERT OVERWRITE TABLE demo_dws.dws_user_order_basic_di PARTITION (dt)
SELECT
  -- 当日下单用户数
  COALESCE(COUNT(DISTINCT o.user_id), 0) AS order_user_count,
  -- 下单用户占比（下单用户/活跃用户）
  CASE 
    WHEN COUNT(DISTINCT a.user_id) = 0 THEN 0.0000
    ELSE COUNT(DISTINCT o.user_id) / COUNT(DISTINCT a.user_id) 
  END AS order_user_rate,
  date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time,
  '${dt}' AS dt  -- 分区字段
FROM daily_active_users a
LEFT JOIN demo_dwd.dwd_o_order o
  ON a.user_id = o.user_id AND o.dt = '${dt}';
