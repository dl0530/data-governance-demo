#!/bin/bash
set -e  # 出错立即退出
dt=${1:-$(date -d '-1 day' +%F)}  # 默认昨天的日期
HIVE_DB="demo_ods"
LOG_FILE="/opt/donglin/data-governance-demo/logs/sqoop_ods_${dt}.log"

# 初始化日志
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行 Sqoop 同步，业务日期：${dt}" > "${LOG_FILE}"

# 同步函数：按表结构严格映射
sync_table() {
  local mysql_table="$1"
  local hive_table="$2"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 同步表：${mysql_table} → ${hive_table} ===" >> "${LOG_FILE}"
  
  # 定义字段顺序和时间字段映射
  case "${mysql_table}" in
    "u_user")
      columns="user_id,username,register_time,update_time"
      map_columns="register_time=String,update_time=String"
      ;;
    "p_product")
      columns="product_id,product_name,price,create_time,update_time"
      map_columns="create_time=String,update_time=String"
      ;;
    "o_order")
      columns="order_id,user_id,total_amount,order_status,create_time,update_time"
      map_columns="create_time=String,update_time=String"
      ;;
    "o_order_detail")
      columns="detail_id,order_id,product_id,quantity,create_time,update_time"
      map_columns="create_time=String,update_time=String"
      ;;
    *)
      echo "[$(date +'%Y-%m-%d %H:%M:%S')] 错误：未知表名 ${mysql_table}" >> "${LOG_FILE}"
      exit 1
      ;;
  esac
  
  # 执行Sqoop导入
  sqoop import \
    --connect "jdbc:mysql://127.0.0.1:3306/demo_oltp" \
    --username "root" \
    --password "Root123!" \
    --table "${mysql_table}" \
    --columns "${columns}" \
    --delete-target-dir \
    --target-dir "/user/hive/warehouse/${HIVE_DB}.db/${hive_table}/dt=${dt}" \
    --fields-terminated-by '\001' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --map-column-java "${map_columns}" \
    -m 1 >> "${LOG_FILE}" 2>&1
  
  # 校验并添加Hive分区
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 校验分区 dt=${dt}" >> "${LOG_FILE}"
  partition_count=$(hive -S -e "USE ${HIVE_DB}; SELECT COUNT(*) FROM ${hive_table} WHERE dt='${dt}' LIMIT 1;" 2>> "${LOG_FILE}")
  
  if [ "${partition_count}" -eq 0 ]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 添加分区 dt=${dt}" >> "${LOG_FILE}"
    hive -e "USE ${HIVE_DB}; ALTER TABLE ${hive_table} ADD PARTITION(dt='${dt}') LOCATION '/user/hive/warehouse/${HIVE_DB}.db/${hive_table}/dt=${dt}';" >> "${LOG_FILE}" 2>&1
  else
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 分区 dt=${dt} 已存在" >> "${LOG_FILE}"
  fi
}

# 同步各表
sync_table "u_user" "ods_u_user"
sync_table "p_product" "ods_p_product"
sync_table "o_order" "ods_o_order"
sync_table "o_order_detail" "ods_o_order_detail"

echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 全量同步完成 ===" >> "${LOG_FILE}"
