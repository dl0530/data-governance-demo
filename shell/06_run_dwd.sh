#!/bin/bash
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

# ====================== 日期参数处理 ======================
# 支持输入日期（格式yyyy-MM-dd），默认处理昨天
if [ $# -eq 1 ]; then
  dt=$1
  # 校验日期格式有效性
  if ! date -d "$dt" +%F >/dev/null 2>&1; then
    echo "错误：日期格式必须为yyyy-MM-dd，例如 2025-11-01"
    exit 1
  fi
else
  dt=$(date -d '-1 day' +%F)  # 默认昨天
  echo "未指定日期，将处理昨天的数据：$dt"
fi

# ====================== 路径配置 ======================
# 基于脚本所在目录定位项目根目录（兼容任意位置执行）
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")  # 项目根目录（data-governance-demo）
SQL_DIR="${PROJECT_ROOT}/sql/hive"          # SQL文件目录
LOG_DIR="${PROJECT_ROOT}/logs"         # 日志目录
LOG_FILE="${LOG_DIR}/dwd_load_${dt}_$(date +%H%M%S).log"  # 日志文件（按日期+时间命名，避免冲突）

# 确保日志目录存在
mkdir -p "$LOG_DIR"

# ====================== 初始化日志 ======================
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 开始执行DWD层加载 === " > "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 处理日期：$dt" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] SQL目录：$SQL_DIR" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 日志文件：$LOG_FILE" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 脚本启动时间：$(date +'%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"

# ====================== 前置检查 ======================
check_hive_connection
check_disk_space

# ====================== 执行SQL（按依赖顺序） ======================
# 执行顺序：用户表 → 商品表 → 订单表（遵循外键依赖）
SQL_FILES=(
  "dwd_u_user.sql"    # 用户表（无依赖）
  "dwd_p_product.sql" # 商品表（无依赖）
  "dwd_o_order.sql"   # 订单表（依赖用户表）
)

for sql_file in "${SQL_FILES[@]}"; do
  sql_path="${SQL_DIR}/${sql_file}"
  table_name=$(echo "$sql_file" | sed 's/\.sql//')  # 提取表名（如dwd_u_user）
  
  # 单个脚本启动时间
  SCRIPT_START=$(date +%s)
  echo "正在执行 ${sql_file} ..."
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行 ${sql_file}" >> "$LOG_FILE"

  # 检查SQL文件是否存在
  if [ ! -f "$sql_path" ]; then
    echo "❌ ${sql_file} 文件不存在！路径：$sql_path"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ❌ ${sql_file} 文件不存在，路径：$sql_path" >> "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    exit 1
  fi

  # 执行Hive SQL，传入dt参数
  hive -hivevar dt="$dt" -f "$sql_path" >> "$LOG_FILE" 2>&1

  # 检查执行结果
  if [ $? -eq 0 ]; then
    SCRIPT_END=$(date +%s)
    SCRIPT_ELAPSED=$(calculate_elapsed_time $SCRIPT_START $SCRIPT_END)
    echo "✅ ${sql_file} 执行成功（耗时：${SCRIPT_ELAPSED}）"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ✅ ${sql_file} 执行成功（耗时：${SCRIPT_ELAPSED}）" >> "$LOG_FILE"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    SCRIPT_END=$(date +%s)
    SCRIPT_ELAPSED=$(calculate_elapsed_time $SCRIPT_START $SCRIPT_END)
    echo "❌ ${sql_file} 执行失败（耗时：${SCRIPT_ELAPSED}，详情见日志：${LOG_FILE}）"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ❌ ${sql_file} 执行失败（耗时：${SCRIPT_ELAPSED}）" >> "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    exit 1  # 失败立即退出，避免后续无效执行
  fi
done

# ====================== 执行汇总 ======================
END_TIME=$(date +%s)
TOTAL_ELAPSED=$(calculate_elapsed_time $START_TIME $END_TIME)
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === DWD层加载执行汇总 ===" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 总执行时长：${TOTAL_ELAPSED}" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 成功脚本数：${SUCCESS_COUNT}" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 失败脚本数：${FAIL_COUNT}" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === DWD层加载全部完成 ===" >> "$LOG_FILE"

# 控制台输出汇总
echo "=== DWD层加载执行汇总 ==="
echo "总执行时长：${TOTAL_ELAPSED}"
echo "成功脚本数：${SUCCESS_COUNT}"
echo "失败脚本数：${FAIL_COUNT}"
echo "=== 全部完成，日志路径：${LOG_FILE} ==="
