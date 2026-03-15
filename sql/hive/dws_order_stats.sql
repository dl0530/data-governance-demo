-- 订单主题DWS层统计脚本
-- 功能：基于dwd_o_order计算订单量、金额等指标
-- 执行方式：
--   1. 默认昨天：hive -f sql/dws_order_stats.sql
--   2. 指定日期：hive -hivevar dt=2025-11-01 -f sql/dws_order_stats.sql
-- 依赖表：demo_dwd.dwd_o_order
-- 输出表：demo_dws.dws_order_stats_di（按dt分区）

-- 定义日期变量（默认昨天）
SET dt=${dt:-$(date -d "yesterday" +%Y-%m-%d)};

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE demo_dws.dws_order_stats_di PARTITION (dt)
SELECT
  -- 当日订单总量
  COALESCE(COUNT(DISTINCT order_id), 0) AS total_order_count,
  -- 当日订单总金额
  COALESCE(SUM(total_amount), 0.00) AS total_order_amount,
  -- 平均订单金额
  CASE 
    WHEN COUNT(DISTINCT order_id) = 0 THEN 0.00
    ELSE SUM(total_amount) / COUNT(DISTINCT order_id) 
  END AS avg_order_amount,
  -- 有效订单量（order_status=1）
  COALESCE(COUNT(DISTINCT CASE WHEN order_status = 1 THEN order_id END), 0) AS valid_order_count,
  -- 有效订单占比
  CASE 
    WHEN COUNT(DISTINCT order_id) = 0 THEN 0.0000
    ELSE COUNT(DISTINCT CASE WHEN order_status = 1 THEN order_id END) 
         / COUNT(DISTINCT order_id) 
  END AS valid_order_rate,
  -- 晚间订单占比（18:00-23:59）
  CASE 
    WHEN COUNT(DISTINCT order_id) = 0 THEN 0.0000
    ELSE COUNT(DISTINCT CASE WHEN hour(create_time) BETWEEN 18 AND 23 THEN order_id END) 
         / COUNT(DISTINCT order_id) 
  END AS night_order_rate,
  -- 人均订单金额
  CASE 
    WHEN COUNT(DISTINCT user_id) = 0 THEN 0.00
    ELSE SUM(total_amount) / COUNT(DISTINCT user_id) 
  END AS per_user_amount,
  date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time,
  '${dt}' AS dt  -- 分区字段
FROM demo_dwd.dwd_o_order
WHERE dt = '${dt}';  -- 仅统计当日订单
