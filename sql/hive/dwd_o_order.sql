-- 订单表DWD层清洗逻辑
-- 功能：
-- 1. 过滤无效数据（订单ID为空的记录）
-- 2. 处理异常值（金额<0或空值修正为0，状态非1/2/3标记为-1）
-- 3. 标准化时间格式（统一为yyyy-MM-dd HH:mm:ss）
-- 4. 补充数据血缘字段（来源系统、ETL时间）

-- 开启动态分区非严格模式（允许仅指定部分分区字段）
SET hive.exec.dynamic.partition.mode=nonstrict;
-- 关闭自动MapJoin（避免小表缓存导致的数据不一致）
SET hive.auto.convert.join=false;
INSERT OVERWRITE TABLE demo_dwd.dwd_o_order PARTITION(dt='${dt}')
SELECT
  order_id,  -- 订单ID（主键）
  user_id,  -- 关联用户ID
  -- 金额异常值处理：负数或空值修正为0.00
  CASE WHEN total_amount < 0 OR total_amount IS NULL THEN 0.00 ELSE total_amount END AS total_amount,
  -- 状态异常值处理：非1/2/3标记为-1
  CASE WHEN order_status IN (1,2,3) THEN order_status ELSE -1 END AS order_status,
  -- 时间转换：原始格式为'yyyy-MM-dd HH:mm:ss'
  from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') AS create_time,
  'mysql_demo_oltp' AS data_source,
  date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
  demo_ods.ods_o_order
WHERE
  dt='${dt}'
  AND order_id IS NOT NULL;  -- 过滤无效主键
