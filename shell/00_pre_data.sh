#!/bin/bash
set -e
dt=${1:-$(date -d '-1 day' +%F)}
echo "=== 开始生成 $dt 的业务数据 ==="

# 清理旧数据
mysql -uroot -pRoot123! -e "
USE demo_oltp;
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE o_order_detail;
TRUNCATE TABLE o_order;
TRUNCATE TABLE p_product;
TRUNCATE TABLE u_user;
SET FOREIGN_KEY_CHECKS=1;
"

# 生成用户表（u_user）数据
mysql -uroot -pRoot123! -e "
USE demo_oltp;
INSERT INTO u_user (username, register_time)
SELECT 
  CONCAT('user_', FLOOR(RAND()*10000)),
  DATE_SUB('$dt', INTERVAL FLOOR(RAND()*365) DAY)
FROM information_schema.tables a, information_schema.tables b
LIMIT 1000;
"

# 生成商品表（p_product）数据
mysql -uroot -pRoot123! -e "
USE demo_oltp;
INSERT INTO p_product (product_name, price, create_time)
SELECT 
  CONCAT('product_', FLOOR(RAND()*1000)),
  ROUND(RAND()*1000, 2),
  DATE_SUB('$dt', INTERVAL FLOOR(RAND()*30) DAY)
FROM information_schema.tables a, information_schema.tables b
LIMIT 500;
"

# 生成订单表（o_order）数据
mysql -uroot -pRoot123! -e "
USE demo_oltp;
INSERT INTO o_order (user_id, total_amount, order_status, create_time)
SELECT 
  FLOOR(RAND()*1000) + 1,  -- 关联 u_user.user_id
  ROUND(RAND()*5000, 2),
  FLOOR(RAND()*3) + 1,
  '$dt'
FROM information_schema.tables a, information_schema.tables b
LIMIT 5000;
"

# 生成订单明细表
mysql -uroot -pRoot123! -e "
USE demo_oltp;
INSERT INTO o_order_detail (order_id, product_id, quantity, create_time)
SELECT 
  FLOOR(RAND()*5000) + 1,  -- 关联 o_order.order_id
  FLOOR(RAND()*500) + 1,   -- 关联 p_product.product_id
  FLOOR(RAND()*10) + 1,
  '$dt'
FROM information_schema.tables a, information_schema.tables b
LIMIT 15000;
"

echo "=== 数据生成完成 ==="
