-- 创建DWS数据库（若不存在）
CREATE DATABASE IF NOT EXISTS demo_dws;
USE demo_dws;

-- 1. 订单主题DWS表（每日统计）
-- 基于DWD订单表（dwd_o_order）聚合，反映当日订单核心指标
CREATE EXTERNAL TABLE IF NOT EXISTS dws_order_stats_di (
  total_order_count INT COMMENT '当日订单总量（去重order_id）',
  total_order_amount DECIMAL(16,2) COMMENT '当日订单总金额（单位：元）',
  avg_order_amount DECIMAL(16,2) COMMENT '当日平均订单金额（总金额/订单总量，无订单时为0）',
  valid_order_count INT COMMENT '当日有效订单量（order_status=1的订单）',
  valid_order_rate DECIMAL(5,4) COMMENT '有效订单占比（有效订单数/总订单数，无订单时为0）',
  night_order_rate DECIMAL(5,4) COMMENT '晚间订单占比（18:00-23:59下单的订单占比，无订单时为0）',
  per_user_amount DECIMAL(16,2) COMMENT '人均订单金额（总金额/下单用户数，无下单用户时为0）',
  etl_time STRING COMMENT 'ETL计算时间（yyyy-MM-dd HH:mm:ss）'
)
PARTITIONED BY (dt STRING COMMENT '统计日期（yyyy-MM-dd）')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_dws.db/dws_order_stats_di';

-- 2. 用户主题DWS表（每日统计）
-- 基于DWD用户表（dwd_u_user）聚合，反映用户增长指标
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user_stats_di (
  new_user_count INT COMMENT '当日新增用户数（register_time在当日的用户）',
  total_user_count INT COMMENT '累计注册用户数（截至当日的所有用户）',
  workhour_register_rate DECIMAL(5,4) COMMENT '工作日注册占比（9:00-18:00注册的新用户占比，无新增时为0）',
  etl_time STRING COMMENT 'ETL计算时间（yyyy-MM-dd HH:mm:ss）'
)
PARTITIONED BY (dt STRING COMMENT '统计日期（yyyy-MM-dd）')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_dws.db/dws_user_stats_di';

-- 3. 商品主题DWS表（每日统计）
-- 基于DWD商品表（dwd_p_product）聚合，反映商品规模及价格分布
CREATE EXTERNAL TABLE IF NOT EXISTS dws_product_stats_di (
  total_product_count INT COMMENT '累计商品总数（截至当日的所有商品）',
  avg_product_price DECIMAL(10,2) COMMENT '商品平均价格（所有商品价格均值，无商品时为0）',
  high_price_rate DECIMAL(5,4) COMMENT '高价商品占比（price≥100元的商品占比，无商品时为0）',
  new_product_count INT COMMENT '当日新增商品数（create_time在当日的商品）',
  etl_time STRING COMMENT 'ETL计算时间（yyyy-MM-dd HH:mm:ss）'
)
PARTITIONED BY (dt STRING COMMENT '统计日期（yyyy-MM-dd）')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_dws.db/dws_product_stats_di';

-- 4. 跨主题（用户+订单）DWS表（每日统计）
-- 关联DWD用户表和订单表，反映用户下单转化效果
CREATE EXTERNAL TABLE IF NOT EXISTS dws_user_order_basic_di (
  order_user_count INT COMMENT '当日下单用户数（有订单记录的用户）',
  order_user_rate DECIMAL(5,4) COMMENT '下单用户占比（下单用户数/当日活跃用户数，无活跃用户时为0）',
  etl_time STRING COMMENT 'ETL计算时间（yyyy-MM-dd HH:mm:ss）'
)
PARTITIONED BY (dt STRING COMMENT '统计日期（yyyy-MM-dd）')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LOCATION '/user/hive/warehouse/demo_dws.db/dws_user_order_basic_di';
