-- 用户表DWD层清洗逻辑
-- 功能：
-- 1. 过滤无效用户（用户ID为空的记录）
-- 2. 清洗用户名（去除前后空格）
-- 3. 标准化注册时间格式
SET hivehive.exec.dynamic.partition.mode=nonstrict;
SET hive.auto.convert.join=false;

INSERT OVERWRITE TABLE demo_dwd.dwd_u_user PARTITION(dt='${dt}')
SELECT
  user_id,  -- 用户ID（主键）
  trim(username) AS username,  -- 去除除用户名前后空格
  -- 时间转换：原始格式为'yyyy-MM-dd HH:mm:ss'，无需处理毫秒
  from_unixtime(unix_timestamp(register_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') AS register_time,
  'mysql_demo_oltp' AS data_source,  -- 数据来源标识
  date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time  -- ETL处理时间
FROM
  demo_ods.ods_u_user
WHERE
  dt='${dt}'  -- 仅处理当前分区
  AND user_id IS NOT NULL;  -- 过滤无效主键
