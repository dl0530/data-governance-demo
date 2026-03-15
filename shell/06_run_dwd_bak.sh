#!/bin/bash
set -e  # 出错立即退出

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
SQL_DIR="${PROJECT_ROOT}/sql"          # SQL文件目录
LOG_DIR="${PROJECT_ROOT}/logs"         # 日志目录
LOG_FILE="${LOG_DIR}/dwd_load_${dt}.log"  # 日志文件（按日期命名）

# 确保日志目录存在
mkdir -p "$LOG_DIR"

# ====================== 初始化日志 ======================
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 开始执行DWD层加载 === " > "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] 处理日期：$dt" >> "$LOG_FILE"
echo "[$(date +'%Y-%m-%d %H:%M:%S')] SQL目录：$SQL_DIR" >> "$LOG_FILE"

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

  echo "正在执行 ${sql_file} ..."
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始执行 ${sql_file}" >> "$LOG_FILE"

  # 执行Hive SQL，传入dt参数
  hive -hivevar dt="$dt" -f "$sql_path" >> "$LOG_FILE" 2>&1

  # 检查执行结果
  if [ $? -eq 0 ]; then
    echo "✅ ${sql_file} 执行成功"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ✅ ${sql_file} 执行成功" >> "$LOG_FILE"
  else
    echo "❌ ${sql_file} 执行失败（详情见日志）"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ❌ ${sql_file} 执行失败" >> "$LOG_FILE"
    exit 1  # 失败立即退出，避免后续无效执行
  fi
done

# ====================== 完成日志 ======================
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === DWD层加载全部完成 ===" >> "$LOG_FILE"
echo "=== 全部完成，日志路径：${LOG_FILE} ==="
