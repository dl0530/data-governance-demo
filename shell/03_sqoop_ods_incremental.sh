#!/bin/bash
set -e  # 出错立即退出

# ====================== 配置与路径处理 ======================
# 日期参数（默认昨天，用于Hive分区）
dt=${1:-$(date -d '-1 day' +%F)}
HIVE_DB="demo_ods"
# 项目根目录（基于脚本所在位置定位，兼容任意执行目录）
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
# 同步时间戳文件（绝对路径，避免相对路径问题）
LAST_SYNC_FILE="${PROJECT_ROOT}/shell/last_sync_time.txt"
# 日志文件（按日期+时间命名，避免覆盖）
LOG_FILE="${PROJECT_ROOT}/logs/sqoop_incremental_${dt}_$(date +%H%M%S).log"

# ====================== 初始化与日志 ======================
# 确保日志目录存在
mkdir -p "${PROJECT_ROOT}/logs"
# 初始化日志
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 开始增量同步（dt=$dt） ===" > "$LOG_FILE"

# 初始化上次同步时间（首次为1970-01-01 00:00:00）
if [ ! -f "$LAST_SYNC_FILE" ]; then
  echo "1970-01-01 00:00:00" > "$LAST_SYNC_FILE"
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 首次同步，初始化上次时间戳文件：$LAST_SYNC_FILE" >> "$LOG_FILE"
fi
last_sync=$(cat "$LAST_SYNC_FILE")
current_sync=$(date +"%Y-%m-%d %H:%M:%S")  # 当前同步时间（精确到秒）
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 同步时间范围：$last_sync → $current_sync" >> "$LOG_FILE"
echo "=== 增量同步（$last_sync → $current_sync） ==="  # 终端输出

# ====================== 增量同步函数 ======================
sync_incremental() {
  local mysql_table="$1"
  local hive_table="$2"
  local log_prefix="[$(date +'%Y-%m-%d %H:%M:%S')] [${mysql_table}]"
  
  echo "$log_prefix 开始增量同步" >> "$LOG_FILE"
  echo "=== 增量同步：$mysql_table → $hive_table ==="  # 终端输出

  # 执行Sqoop增量导入
  sqoop import \
    --connect "jdbc:mysql://127.0.0.1:3306/demo_oltp?useSSL=false&serverTimezone=UTC" \
    --username "root" \
    --password "Root123!" \
    --table "$mysql_table" \
    --where "update_time >= '$last_sync' AND update_time < '$current_sync'" \
    --target-dir "/user/hive/warehouse/${HIVE_DB}.db/${hive_table}/dt=$dt" \
    --fields-terminated-by '\001' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --append \
    -m 2 >> "$LOG_FILE" 2>&1

  # 同步成功后，添加Hive分区（若不存在）
  if [ $? -eq 0 ]; then
    echo "$log_prefix Sqoop同步成功，开始添加Hive分区" >> "$LOG_FILE"
    hive -e "USE ${HIVE_DB}; ALTER TABLE ${hive_table} ADD IF NOT EXISTS PARTITION(dt='$dt');" >> "$LOG_FILE" 2>&1
    echo "$log_prefix Hive分区添加完成" >> "$LOG_FILE"
  else
    echo "$log_prefix Sqoop同步失败，终止执行" >> "$LOG_FILE"
    exit 1  # 同步失败则退出，避免更新时间戳
  fi
}

# ====================== 执行同步 ======================
sync_incremental "u_user" "ods_u_user"
sync_incremental "p_product" "ods_p_product"
sync_incremental "o_order" "ods_o_order"
sync_incremental "o_order_detail" "ods_o_order_detail"

# ====================== 收尾工作 ======================
echo "$current_sync" > "$LAST_SYNC_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 增量同步全部完成，更新时间戳为：$current_sync" >> "$LOG_FILE"
echo "=== 增量同步完成 ==="
echo "日志路径：$LOG_FILE"
