-- 增量同步数据

USE demo_dwd;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dwd_o_order_detail PARTITION(dt)
SELECT 
    t.detail_id,
    t.order_id,
    t.product_id,
    t.quantity,
    t.create_time,
    'mysql_demo_oltp' AS data_source,
    from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS etl_time,
    t.dt
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY detail_id ORDER BY ts_op DESC) AS rn
    FROM demo_ods.ods_o_order_detail
    WHERE 
        dt = '${SYNC_DATE}'  -- 占位符，由Shell动态替换,如手动执行改为实际同步日期（yyyy-MM-dd）
        AND op_type != 'DELETE'
) t
WHERE t.rn = 1;
