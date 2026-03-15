-- 商品表DWD层清洗逻辑
-- 功能：
-- 1. 过滤无效商品（商品ID为空的记录）
-- 2. 清洗商品名称（去除前后空格）
-- 3. 处理异常价格（价格<0修正为0.00）
-- 4. 标准化创建时间格式
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.auto.convert.join=false;

INSERT OVERWRITE TABLE demo_dwd.dwd_p_product PARTITION(dt='${dt}')
SELECT
  product_id,  -- 商品ID（主键）
  trim(product_name) AS product_name,  -- 去除名称前后空格
  -- 价格异常值处理：负数修正为0.00
  CASE WHEN price < 0 THEN 0.00 ELSE price END AS price,
  -- 时间转换：原始格式为'yyyy-MM-dd HH:mm:ss'
  from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') AS create_time,
  'mysql_demo_oltp' AS data_source,
  date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
  demo_ods.ods_p_product
WHERE
  dt='${dt}'
  AND product_id IS NOT NULL;  -- 过滤无效主键
