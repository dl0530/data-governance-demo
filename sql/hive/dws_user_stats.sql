-- 用户主题DWS层统计脚本
-- 功能：基于dwd_u_user计算用户新增、累计等指标
-- 执行方式：
--   1. 默认昨天：hive -f sql/dws_user_stats.sql
--   2. 指定日期：hive -hivevar dt=2025-11-01 -f sql/dws_user_stats.sql
-- 依赖表：demo_dwd.dwd_u_user
-- 输出表：demo_dws.dws_user_stats_di（按dt分区）

-- 定义日期变量（默认昨天，若外部传入则覆盖）
SET dt=${dt:-$(date -d "yesterday" +%Y-%m-%d)};

-- 开启动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE demo_dws.dws_user_stats_di PARTITION (dt)
SELECT
  -- 当日新增用户数（注册时间在dt当天）
  COALESCE(COUNT(DISTINCT CASE WHEN date(register_time) = date('${dt}') THEN user_id END), 0) AS new_user_count,
  -- 累计注册用户数（截至dt的所有用户）
  COALESCE(COUNT(DISTINCT user_id), 0) AS total_user_count,
  -- 工作日注册占比（9:00-18:00注册的新用户）
  CASE 
    WHEN COUNT(DISTINCT CASE WHEN date(register_time) = date('${dt}') THEN user_id END) = 0 THEN 0.0000
    ELSE COUNT(DISTINCT CASE WHEN date(register_time) = date('${dt}')
                              AND hour(register_time) BETWEEN 9 AND 18
                            THEN user_id END)
         / COUNT(DISTINCT CASE WHEN date(register_time) = date('${dt}') THEN user_id END)
  END AS workhour_register_rate,
  -- ETL计算时间
  date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time,
  '${dt}' AS dt  -- 分区字段
FROM demo_dwd.dwd_u_user
WHERE dt <= '${dt}';  -- 包含历史数据计算累计值。
