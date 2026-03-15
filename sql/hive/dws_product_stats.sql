-- 商品主题DWS层统计脚本
-- 功能：基于dwd_p_product计算商品总量、价格分布等指标
-- 执行方式：
--   1. 默认昨天：hive -f sql/dws_product_stats.sql
--   2. 指定日期：hive -hivevar dt=2025-11-01 -f sql/dws_product_stats.sql
-- 依赖表：demo_dwd.dwd_p_product
-- 输出表：demo_dws.dws_product_stats_di（按dt分区）

-- 定义日期变量（默认昨天）
SET dt=${dt:-$(date -d "yesterday" +%Y-%m-%d)};

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE demo_dws.dws_product_stats_di PARTITION (dt)
SELECT
  -- 累计商品总数（截至dt的所有商品）
  COALESCE(COUNT(DISTINCT product_id), 0) AS total_product_count,
  -- 商品平均价格
  CASE 
    WHEN COUNT(DISTINCT product_id) = 0 THEN 0.00
    ELSE AVG(price) 
  END AS avg_product_price,
  -- 高价商品占比（price≥100元）
  CASE 
    WHEN COUNT(DISTINCT product_id) = 0 THEN 0.0000
    ELSE COUNT(DISTINCT CASE WHEN price >= 100 THEN product_id END)
         / COUNT(DISTINCT product_id) 
  END AS high_price_rate,
  -- 当日新增商品数（create_time在dt当天）
  COALESCE(COUNT(DISTINCT CASE WHEN date(create_time) = date('${dt}') THEN product_id END), 0) AS new_product_count,
  date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time,
  '${dt}' AS dt  -- 分区字段
FROM demo_dwd.dwd_p_product
WHERE dt <= '${dt}'; 
