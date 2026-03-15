-- 订单表：ods_o_order → dwd_o_order（含金额/状态校验）

USE demo_dwd;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dwd_o_order PARTITION(dt)
SELECT 
    t.order_id,
    t.user_id,
    CASE WHEN t.total_amount < 0 THEN 0 ELSE t.total_amount END AS total_amount,  -- 金额非负校验
    CASE WHEN t.order_status IN (1,2,3) THEN t.order_status ELSE -1 END AS order_status,  -- 状态合法性校验
    t.create_time,
    'mysql_demo_oltp' AS data_source,
    from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS etl_time,
    t.dt
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ts_op DESC) AS rn
    FROM demo_ods.ods_o_order
    WHERE 
        dt = '${SYNC_DATE}'  -- 占位符，由Shell动态替换,如手动执行改为实际同步日期（yyyy-MM-dd）
        AND op_type != 'DELETE'
) t
WHERE t.rn = 1;
