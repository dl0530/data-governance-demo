-- 用户表：ods_u_user → dwd_u_user

USE demo_dwd;
-- 开启动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 100;

-- 增量同步：覆盖当日分区，保留最新数据
INSERT OVERWRITE TABLE dwd_u_user PARTITION(dt)
SELECT 
    t.user_id,
    t.username,
    t.register_time,
    'mysql_demo_oltp' AS data_source,  -- 数据来源标识
    from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS etl_time,  -- ETL 执行时间
    t.dt
FROM (
    SELECT 
        *,
        -- 按主键分组，取最新操作的一条数据
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts_op DESC) AS rn
    FROM demo_ods.ods_u_user
    WHERE 
        dt = '${SYNC_DATE}'  -- 占位符，由Shell动态替换,如手动执行改为实际同步日期（yyyy-MM-dd）
        AND op_type != 'DELETE'  -- 剔除删除数据
) t
WHERE t.rn = 1;  -- 仅保留最新版本
