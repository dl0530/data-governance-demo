-- 写入 ods_u_user（保留全量操作类型）
INSERT INTO ods_u_user
SELECT 
    t.user_id,
    t.username,
    t.register_time,
    t.update_time,
    k.type AS op_type,                  -- 操作类型：INSERT/UPDATE/DELETE
    FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss') AS ts_op,  -- 操作时间
    DATE_FORMAT(TO_TIMESTAMP(t.register_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS dt
FROM kafka_canal_source_user k,
     UNNEST(k.data) AS t(user_id, username, register_time, update_time)
WHERE 
    k.database = 'demo_oltp' 
    AND k.`table` = 'u_user' 
    AND t.user_id IS NOT NULL;
