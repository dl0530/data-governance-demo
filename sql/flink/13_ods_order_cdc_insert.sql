-- 写入 ods_o_order（保留全量操作类型）
INSERT INTO ods_o_order
SELECT 
    t.order_id,
    t.user_id,
    t.total_amount,
    t.order_status,
    t.create_time,
    t.update_time,
    k.type AS op_type,
    FROM_UNIXTIME(k.ts/1000,'yyyy-MM-dd HH:mm:ss') AS ts_op,
    DATE_FORMAT(TO_TIMESTAMP(t.create_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt
FROM kafka_canal_source_order k,
     UNNEST(k.data) AS t(order_id, user_id, total_amount, order_status, create_time, update_time)
WHERE 
    k.database = 'demo_oltp' 
    AND k.`table` = 'o_order' 
    AND t.order_id IS NOT NULL;
