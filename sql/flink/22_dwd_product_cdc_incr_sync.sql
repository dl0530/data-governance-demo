-- 商品表：ods_p_product → dwd_p_product（含价格校验）

USE demo_dwd;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dwd_p_product PARTITION(dt)
SELECT 
    t.product_id,
    t.product_name,
    CASE WHEN t.price < 0 THEN 0 ELSE t.price END AS price,  -- 价格非负校验
    t.create_time,
    'mysql_demo_oltp' AS data_source,
    from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS etl_time,
    t.dt
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ts_op DESC) AS rn
    FROM demo_ods.ods_p_product
    WHERE 
        dt = '${SYNC_DATE}'  -- 占位符，由Shell动态替换,如手动执行改为实际同步日期（yyyy-MM-dd）
        AND op_type != 'DELETE'
) t
WHERE t.rn = 1;

