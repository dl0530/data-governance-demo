-- 写入 ods_p_product（保留全量操作类型）
INSERT INTO ods_p_product
SELECT 
    t.product_id,
    t.product_name,
    t.price,
    t.create_time,
    t.update_time,
    k.type AS op_type,
    FROM_UNIXTIME(k.ts/1000,'yyyy-MM-dd HH:mm:ss') AS ts_op,
    DATE_FORMAT(TO_TIMESTAMP(t.create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt
FROM kafka_canal_source_product k,
     UNNEST(k.data) AS t(product_id, product_name, price, create_time, update_time)
WHERE 
    k.database = 'demo_oltp' 
    AND k.`table` = 'p_product' 
    AND t.product_id IS NOT NULL;
