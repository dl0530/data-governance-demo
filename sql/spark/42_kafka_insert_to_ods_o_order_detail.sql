-- 功能：将解析后的o_order_detail增量数据，追加写入Hive ODS层分区表
-- 核心配置：关闭Hive动态分区严格模式（会话级，仅当前生效）
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 切换到ODS层库（避免表名前缀）
USE demo_ods;

-- 增量写入：INSERT INTO（追加模式，不覆盖历史数据）
INSERT INTO ods_o_order_detail (detail_id, order_id, product_id, quantity, create_time, update_time, dt)
SELECT
  CAST(detail_id AS INT) AS detail_id,            # 强转INT匹配Hive表
  CAST(order_id AS INT) AS order_id,              # 强转INT匹配Hive表
  CAST(product_id AS INT) AS product_id,          # 强转INT匹配Hive表
  CAST(quantity AS INT) AS quantity,              # 强转INT匹配Hive表
  create_time,
  update_time,
  -- 毫秒级时间戳→dt分区（yyyy-MM-dd，数仓标准按天分区）
  FROM_UNIXTIME(CAST(op_ts / 1000 AS BIGINT), 'yyyy-MM-dd') AS dt
FROM canal_o_order_detail_temp;

-- 验证步骤（执行完写入后，手动执行以下SQL验证）
-- 1. 查看创建的分区：SHOW PARTITIONS ods_o_order_detail;
-- 2. 查看分区内数据：SELECT * FROM ods_o_order_detail WHERE dt = '2026-02-01';（替换为实际日期）
