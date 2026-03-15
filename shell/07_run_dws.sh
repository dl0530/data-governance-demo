#!/bin/bash
# DWS层批量执行脚本
# 功能：按依赖顺序执行DWS脚本，默认处理昨天日期，支持手动指定日期
# 使用方式：
#   1. 执行昨天数据：sh 07_run_dws.sh
#   2. 执行指定日期：sh 07_run_dws.sh 2025-11-01
set -e  # 出错立即退出

# ====================== 全局变量初始化 ======================
START_TIME=$(date +%s)  # 脚本启动时间（秒级）
SUCCESS_COUNT=0         # 成功执行的脚本数
FAIL_COUNT=0            # 失败执行的脚本数

# ====================== 函数定义 ======================
# 1. 检查Hive服务连通性
check_hive_connection() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 检查Hive服务连通性..." >> "$LOG_FILE"
  if ! hive -e "show databases;" >/dev/null 2>&1; then
    echo "[ERROR] Hive服务连接失败，请检查Hive配置和服务状态！"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [ERROR] Hive服务连接失败" >> "$LOG_FILE"
    exit 1
  fi
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] Hive服务连通性检查通过" >> "$LOG_FILE"
}

# 2. 检查磁盘空间（阈值：可用空间<10GB则告警）
check_disk_space() {
  LOG_DISK=$(df -P "$LOG_DIR" | awk 'NR==2 {print $4}' | numfmt --from=auto)  # 可用空间（KB）
  SQL_DISK=$(df -P "$SQL_DIR" | awk 'NR==2 {print $4}' | numfmt --from=auto)   # 可用空间（KB）
  MIN_SPACE=$((10 * 1024 * 1024))  # 10GB（KB）

  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 检查磁盘空间..." >> "$LOG_FILE"
  if [ "$LOG_DISK" -lt "$MIN_SPACE" ]; then
    echo "[WARNING] 日志目录所在磁盘可用空间不足10GB，当前可用：$((LOG_DISK/1024/1024))GB"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [WARNING] 日志目录磁盘可用空间不足：$((LOG_DISK/1024/1024))GB" >> "$LOG_FILE"
  fi
  if [ "$SQL_DISK" -lt "$MIN_SPACE" ]; then
    echo "[WARNING] SQL目录所在磁盘可用空间不足10GB，当前可用：$((SQL_DISK/1024/1024))GB"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [WARNING] SQL目录磁盘可用空间不足：$((SQL_DISK/1024/1024))GB" >> "$LOG_FILE"
  fi
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 磁盘空间检查完成" >> "$LOG_FILE"
}

# 3. 计算耗时（秒转 时:分:秒）
calculate_elapsed_time() {
  local start=$1
  local end=$2
  local elapsed=$((end - start))
  printf "%02d:%02d:%02d" $((elapsed/3600)) $((elapsed%3600/60)) $((elapsed%60))
}

# ==================== 配置区 ====================
# 默认处理昨天日期，支持外部传入日期（格式：yyyy-MM-dd）
dt=${1:-$(date -d '-1 day' +%F)}
# 校验日期格式有效性
if ! date -d "$dt" +%F >/dev/null 2>&1; then
  echo "错误：日期格式必须为yyyy-MM-dd，例如 2025-11-01"
  exit 1
fi

# 日志目录（统一一层目录，自动创建）
LOG_DIR="/opt/donglin/data-governance-demo/logs"
# SQL脚本目录（改为绝对路径）
SQL_DIR="/opt/donglin/data-governance-demo/sql/hive"
# 执行顺序（严格按依赖：用户→商品→订单→跨主题）
scripts=(
  "dws_user_stats.sql"
  "dws_product_stats.sql"
  "dws_order_stats.sql"
  "dws_user_order_basic.sql"
)

# ==================== 执行区 ====================
# 初始化日志文件（按日期+批次命名，避免冲突）
LOG_FILE="${LOG_DIR}/dws_all_${dt}_$(date +%H%M%S).log"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === DWS层批量执行开始 ===" > "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 业务日期：${dt}" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 脚本目录：${SQL_DIR}" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 执行顺序：${scripts[*]}" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 日志文件：${LOG_FILE}" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 脚本启动时间：$(date +'%Y-%m-%d %H:%M:%S')" >> "${LOG_FILE}"

# 检查日志目录是否存在
mkdir -p "${LOG_DIR}"

# 前置检查
check_hive_connection
check_disk_space

# 逐个执行脚本
for script in "${scripts[@]}"; do
  # 提取脚本名（不含扩展名），用于日志区分
  script_name=$(basename "$script" .sql)
  # 单个脚本启动时间
  SCRIPT_START=$(date +%s)
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 开始执行：${script} ===" >> "${LOG_FILE}"

  # 检查SQL文件是否存在
  sql_path="${SQL_DIR}/${script}"
  if [ ! -f "$sql_path" ]; then
    echo "[ERROR] ${script} 文件不存在！路径：$sql_path"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] === ${script} 文件不存在！路径：$sql_path ===" >> "${LOG_FILE}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    exit 1
  fi

  # 执行Hive脚本，传入日期参数，输出日志到总日志文件
  hive -hivevar dt="${dt}" -f "${sql_path}" >> "${LOG_FILE}" 2>&1

  # 检查执行结果
  if [ $? -eq 0 ]; then
    SCRIPT_END=$(date +%s)
    SCRIPT_ELAPSED=$(calculate_elapsed_time $SCRIPT_START $SCRIPT_END)
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] === ${script} 执行成功（耗时：${SCRIPT_ELAPSED}） ===" >> "${LOG_FILE}"
    echo "[INFO] ${script} 执行成功（日期：${dt}，耗时：${SCRIPT_ELAPSED}）"  # 控制台实时输出
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    SCRIPT_END=$(date +%s)
    SCRIPT_ELAPSED=$(calculate_elapsed_time $SCRIPT_START $SCRIPT_END)
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] === ${script} 执行失败！（耗时：${SCRIPT_ELAPSED}） ===" >> "${LOG_FILE}"
    echo "[ERROR] ${script} 执行失败，详情见日志：${LOG_FILE}（耗时：${SCRIPT_ELAPSED}）"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    exit 1  # 前序脚本失败，终止后续执行
  fi
done

# 执行完成 - 汇总统计
END_TIME=$(date +%s)
TOTAL_ELAPSED=$(calculate_elapsed_time $START_TIME $END_TIME)
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === DWS层执行汇总 ===" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 总执行时长：${TOTAL_ELAPSED}" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 成功脚本数：${SUCCESS_COUNT}" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 失败脚本数：${FAIL_COUNT}" >> "${LOG_FILE}"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === DWS层全部脚本执行成功 ===" >> "${LOG_FILE}"

# 控制台输出汇总
echo "[SUCCESS] 所有DWS层脚本执行完成（日期：${dt}）"
echo "=== DWS层执行汇总 ==="
echo "总执行时长：${TOTAL_ELAPSED}"
echo "成功脚本数：${SUCCESS_COUNT}"
echo "失败脚本数：${FAIL_COUNT}"
echo "日志文件：${LOG_FILE}"
