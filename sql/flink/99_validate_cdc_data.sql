-- 验证方案（模拟测试）
-- 数据生产验证
-- 1. 在 MySQL 业务库执行 INSERT/UPDATE/DELETE 操作；
-- 2. 查看 Kafka 主题 `canal_demo_oltp` 是否有对应日志（命令：`kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic canal_demo_oltp --from-beginning`）；

-- ODS 层验证
-- 查看 ODS 表是否捕获全量操作
SELECT op_type, COUNT(1) FROM demo_ods.ods_u_user WHERE dt = '2026-03-15' GROUP BY op_type;
SELECT op_type, COUNT(1) FROM demo_ods.ods_p_product WHERE dt = '2026-03-15' GROUP BY op_type;

-- DWD 层验证
-- 验证是否仅保留最新数据（无重复主键）
SELECT user_id, COUNT(1) FROM demo_dwd.dwd_u_user WHERE dt = '2026-03-15' GROUP BY user_id HAVING COUNT(1) > 1;

-- 验证字段校验逻辑
SELECT price FROM demo_dwd.dwd_p_product WHERE dt = '2026-03-15' AND price < 0;  -- 应无结果
SELECT order_status FROM demo_dwd.dwd_o_order WHERE dt = '2026-03-15' AND order_status NOT IN (1,2,3,-1);  -- 应无结果
