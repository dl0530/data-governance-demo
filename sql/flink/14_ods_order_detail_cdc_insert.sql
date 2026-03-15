-- 写入 ods_o_order_detail（保留全量操作类型）
INSERT INTO ods_o_order_detail
SELECT 
    t.detail_id,
    t.order_id,
    t.product_id,
    t.quantity,
    t.create_time,
    t.update_time,
    k.type AS op_type,
    FROM_UNIXTIME(k.ts/1000,'yyyy-MM-dd HH:mm:ss') AS ts_op,
    DATE_FORMAT(TO_TIMESTAMP(t.create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt
FROM kafka_canal_source_order_detail k,
     UNNEST(k.data) AS t(detail_id, order_id, product_id, quantity, create_time, update_time)
WHERE 
    k.database = 'demo_oltp' 
    AND k.`table` = 'o_order_detail' 
    AND t.detail_id IS NOT NULL;
