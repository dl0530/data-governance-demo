/*
新增字段会默认填充NULL，原有数据不受影响；
是为了 Flink 同步的 UD 数据会填充这两个字段，Sqoop 全量数据这两个字段为NULL。
op_type STRING COMMENT '操作类型：INSERT/UPDATE/DELETE',
op_ts STRING COMMENT '操作时间：yyyy-MM-dd HH:mm:ss'
*/

CREATE DATABASE IF NOT EXISTS demo_ods;
USE demo_ods;

-- 对应 MySQL 的 u_user 表
CREATE EXTERNAL TABLE IF NOT EXISTS ods_u_user (
  user_id INT COMMENT '用户ID',
  username STRING COMMENT '用户名',
  register_time STRING COMMENT '注册时间',
  update_time STRING COMMENT '更新时间',
  op_type STRING COMMENT '操作类型：INSERT/UPDATE/DELETE',
  op_ts STRING COMMENT '操作时间：yyyy-MM-dd HH:mm:ss'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_ods.db/ods_u_user';

-- 对应 MySQL 的 p_product 表
CREATE EXTERNAL TABLE IF NOT EXISTS ods_p_product (
  product_id INT COMMENT '商品ID',
  product_name STRING COMMENT '商品名称',
  price DECIMAL(10,2) COMMENT '商品价格',
  create_time STRING COMMENT '创建时间',
  update_time STRING COMMENT '更新时间',
  op_type STRING COMMENT '操作类型：INSERT/UPDATE/DELETE',
  op_ts STRING COMMENT '操作时间：yyyy-MM-dd HH:mm:ss'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_ods.db/ods_p_product';

-- 对应 MySQL 的 o_order 表
CREATE EXTERNAL TABLE IF NOT EXISTS ods_o_order (
  order_id INT COMMENT '订单ID',
  user_id INT COMMENT '用户ID',
  total_amount DECIMAL(10,2) COMMENT '总金额',
  order_status TINYINT COMMENT '订单状态',
  create_time STRING COMMENT '创建时间',
  update_time STRING COMMENT '更新时间',
  op_type STRING COMMENT '操作类型：INSERT/UPDATE/DELETE',
  op_ts STRING COMMENT '操作时间：yyyy-MM-dd HH:mm:ss'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_ods.db/ods_o_order';

-- 对应 MySQL 的 order_detail 表（加前缀 o_，保持统一）
CREATE EXTERNAL TABLE IF NOT EXISTS ods_o_order_detail (
  detail_id INT COMMENT '明细ID',
  order_id INT COMMENT '订单ID',
  product_id INT COMMENT '商品ID',
  quantity INT COMMENT '购买数量',
  create_time STRING COMMENT '创建时间',
  update_time STRING COMMENT '更新时间',
  op_type STRING COMMENT '操作类型：INSERT/UPDATE/DELETE',
  op_ts STRING COMMENT '操作时间：yyyy-MM-dd HH:mm:ss'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_ods.db/ods_o_order_detail';
