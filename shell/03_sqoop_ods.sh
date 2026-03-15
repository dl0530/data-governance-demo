#!/bin/bash
set -e  # 出错立即退出
dt=${1:-$(date -d '-1 day' +%F)}  # 默认昨天的日期
HIVE_DB="demo_ods"
LOG_FILE="/opt/donglin/data-governance-demo/logs/sqoop_ods_${dt}.log"

# ======================================
# 【监控埋点-新增】初始化监控变量（仅新增，不影响原有逻辑）
# ======================================
# 记录脚本整体启动时间（秒级）
START_TIME=$(date +%s)
# 业务标签（用于监控区分该同步任务）
BUSINESS_TAG="sqoop_ods_full_sync"
# 指标上报脚本路径（适配你的项目）
REPORTER_SCRIPT="/opt/donglin/data-governance-demo/shell/metrics_reporter.sh"
# 初始化各表同步行数存储变量
declare -A TABLE_SYNC_ROWS
TABLE_SYNC_ROWS["u_user"]=0
TABLE_SYNC_ROWS["p_product"]=0
TABLE_SYNC_ROWS["o_order"]=0
TABLE_SYNC_ROWS["o_order_detail"]=0

# 初始化日志
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行 Sqoop 同步，业务日期：${dt}" > "${LOG_FILE}"

# 同步函数：按表结构严格映射（保留原版所有逻辑，仅新增行数提取）
sync_table() {
  local mysql_table="$1"
  local hive_table="$2"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 同步表：${mysql_table} → ${hive_table} ===" >> "${LOG_FILE}"
  
  # 定义字段顺序和时间字段映射（原版逻辑完全保留）
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
  
  # 执行Sqoop导入（原版逻辑完全保留）
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
  
  # ======================================
  # 【监控埋点-新增】提取当前表同步行数（不影响原有逻辑）
  # ======================================
  # 从Hive表直接计数
local sync_rows=$(hive -S -e "USE ${HIVE_DB}; SELECT COUNT(*) FROM ${hive_table} WHERE dt='${dt}';" 2>> "${LOG_FILE}")
# 容错处理：行数为空/非数字时赋值0
if [ -z "${sync_rows}" ] || ! [[ "${sync_rows}" =~ ^[0-9]+$ ]]; then
  sync_rows=0
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 表${mysql_table}从Hive计数失败，默认赋值0" >> "${LOG_FILE}"
else
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 表${mysql_table}同步行数：${sync_rows}" >> "${LOG_FILE}"
fi

  TABLE_SYNC_ROWS["${mysql_table}"]=${sync_rows}
  
  # 校验并添加Hive分区（原版逻辑完全保留）
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 校验分区 dt=${dt}" >> "${LOG_FILE}"
  partition_count=$(hive -S -e "USE ${HIVE_DB}; SELECT COUNT(*) FROM ${hive_table} WHERE dt='${dt}' LIMIT 1;" 2>> "${LOG_FILE}")
  
  if [ "${partition_count}" -eq 0 ]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 添加分区 dt=${dt}" >> "${LOG_FILE}"
    hive -e "USE ${HIVE_DB}; ALTER TABLE ${hive_table} ADD PARTITION(dt='${dt}') LOCATION '/user/hive/warehouse/${HIVE_DB}.db/${hive_table}/dt=${dt}';" >> "${LOG_FILE}" 2>&1
  else
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 分区 dt=${dt} 已存在" >> "${LOG_FILE}"
  fi
}

# 同步各表（原版逻辑完全保留）
sync_table "u_user" "ods_u_user"
sync_table "p_product" "ods_p_product"
sync_table "o_order" "ods_o_order"
sync_table "o_order_detail" "ods_o_order_detail"

echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 全量同步完成 ===" >> "${LOG_FILE}"

# ======================================
# 【监控埋点-新增】上报所有监控指标（仅新增，不影响原有逻辑）
# ======================================
# 计算脚本整体耗时（秒）
END_TIME=$(date +%s)
TOTAL_COST_TIME=$((END_TIME - START_TIME))

# 1. 上报整体同步成功率（因为set -e，能执行到这里说明全量成功）
${REPORTER_SCRIPT} "data_warehouse_success_rate" 1 "${BUSINESS_TAG}" "${dt}"

# 2. 上报整体同步耗时
${REPORTER_SCRIPT} "data_warehouse_cost_time_seconds" "${TOTAL_COST_TIME}" "${BUSINESS_TAG}" "${dt}"

# 3. 上报各表同步行数（按表维度区分）
for table in "u_user" "p_product" "o_order" "o_order_detail"; do
  ${REPORTER_SCRIPT} "data_warehouse_sync_rows" "${TABLE_SYNC_ROWS[${table}]}" "${BUSINESS_TAG}_${table}" "${dt}"
done

# 4. 上报全量表总同步行数
TOTAL_ROWS=$((TABLE_SYNC_ROWS["u_user"] + TABLE_SYNC_ROWS["p_product"] + TABLE_SYNC_ROWS["o_order"] + TABLE_SYNC_ROWS["o_order_detail"]))
${REPORTER_SCRIPT} "data_warehouse_sync_total_rows" "${TOTAL_ROWS}" "${BUSINESS_TAG}" "${dt}"

# 日志记录监控上报结果
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 监控指标上报完成：总耗时${TOTAL_COST_TIME}秒，总同步行数${TOTAL_ROWS}" >> "${LOG_FILE}"
