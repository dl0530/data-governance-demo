#!/bin/bash
# DWS层批量执行脚本
# 功能：按依赖顺序执行DWS脚本，默认处理昨天日期，支持手动指定日期
# 使用方式：
#   1. 执行昨天数据：sh 07_run_dws.sh
#   2. 执行指定日期：sh 07_run_dws.sh 2025-11-01
set -e  # 出错立即退出

# ==================== 配置区 ====================
# 默认处理昨天日期，支持外部传入日期（格式：yyyy-MM-dd）
dt=${1:-$(date -d '-1 day' +%F)}
# 日志目录（统一一层目录，自动创建）
LOG_DIR="/opt/donglin/data-governance-demo/logs"
# SQL脚本目录（改为绝对路径）
SQL_DIR="/opt/donglin/data-governance-demo/sql"
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

# 检查日志目录是否存在
mkdir -p "${LOG_DIR}"

# 逐个执行脚本
for script in "${scripts[@]}"; do
  # 提取脚本名（不含扩展名），用于日志区分
  script_name=$(basename "$script" .sql)
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 开始执行：${script} ===" >> "${LOG_FILE}"

  # 执行Hive脚本，传入日期参数，输出日志到总日志文件
  hive -hivevar dt="${dt}" -f "${SQL_DIR}/${script}" >> "${LOG_FILE}" 2>&1

  # 检查执行结果
  if [ $? -eq 0 ]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] === ${script} 执行成功 ===" >> "${LOG_FILE}"
    echo "[INFO] ${script} 执行成功（日期：${dt}）"  # 控制台实时输出
  else
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] === ${script} 执行失败！ ===" >> "${LOG_FILE}"
    echo "[ERROR] ${script} 执行失败，详情见日志：${LOG_FILE}"
    exit 1  # 前序脚本失败，终止后续执行
  fi
done

# 执行完成
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === DWS层全部脚本执行成功 ===" >> "${LOG_FILE}"
echo "[SUCCESS] 所有DWS层脚本执行完成（日期：${dt}），日志：${LOG_FILE}"
