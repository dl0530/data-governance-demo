-- 创建 DWD 库（若不存在）
CREATE DATABASE IF NOT EXISTS demo_dwd;
USE demo_dwd;

-- 1. DWD 用户表（对应 ODS 的 ods_u_user）
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_u_user (
  user_id INT COMMENT '用户ID（主键，非空）',
  username STRING COMMENT '用户名（去空格，非空）',
  register_time STRING COMMENT '注册时间（标准化为 yyyy-MM-dd HH:mm:ss）',
  data_source STRING COMMENT '数据来源（固定为 mysql_demo_oltp）',
  etl_time STRING COMMENT 'ETL 处理时间'
)
PARTITIONED BY (dt STRING COMMENT '分区日期，与 ODS 层对齐')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'  -- 与 ODS 层分隔符一致，便于导入
LOCATION '/user/hive/warehouse/demo_dwd.db/dwd_u_user';

-- 2. DWD 商品表（对应 ODS 的 ods_p_product）
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_p_product (
  product_id INT COMMENT '商品ID（主键，非空）',
  product_name STRING COMMENT '商品名称（去空格，非空）',
  price DECIMAL(10,2) COMMENT '商品价格（异常值处理：<0 设为 0）',
  create_time STRING COMMENT '创建时间（标准化）',
  data_source STRING COMMENT '数据来源',
  etl_time STRING COMMENT 'ETL 处理时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_dwd.db/dwd_p_product';

-- 3. DWD 订单表（对应 ODS 的 ods_o_order）
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_o_order (
  order_id INT COMMENT '订单ID（主键，非空）',
  user_id INT COMMENT '用户ID（关联 dwd_u_user，非空）',
  total_amount DECIMAL(10,2) COMMENT '订单总金额（异常值处理：<0 设为 0）',
  order_status TINYINT COMMENT '订单状态（清洗：非 1/2/3 设为 -1 标记异常）',
  create_time STRING COMMENT '创建时间（标准化）',
  data_source STRING COMMENT '数据来源',
  etl_time STRING COMMENT 'ETL 处理时间'
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_dwd.db/dwd_o_order';

-- 4.DWD 订单明细表
CREATE TABLE IF NOT EXISTS dwd_o_order_detail (
    detail_id INT COMMENT '明细ID（自增主键）',
    order_id INT COMMENT '关联订单ID（关联dwd_o_order）',
    product_id INT COMMENT '关联商品ID（关联dwd_p_product）',
    quantity INT COMMENT '购买数量',
    create_time STRING COMMENT '创建时间（yyyy-MM-dd HH:mm:ss）',
    data_source STRING COMMENT '数据来源：mysql_demo_oltp',
    etl_time STRING COMMENT 'ETL执行时间'
) PARTITIONED BY (dt STRING COMMENT '分区字段（同步日期：yyyy-MM-dd）')  -- 仅此处声明dt
STORED AS ORC  -- ORC 格式提升查询性能
TBLPROPERTIES ('orc.compress' = 'SNAPPY');
