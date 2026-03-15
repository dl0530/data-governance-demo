#!/bin/bash
set -e
dt=${1:-$(date -d '-1 day' +%F)}
HIVE_DB="demo_ods"
HIVE_CMD="/opt/bigdata/hive-3.1.2/bin/hive"
TABLES=("ods_u_user" "ods_p_product" "ods_o_order" "ods_o_order_detail")
LOG_FILE="/opt/donglin/data-governance-demo/logs/partition_check_${dt}.log"

# 初始化日志
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 开始校验 Hive ODS层 分区：$dt ===" > "$LOG_FILE"

for tbl in "${TABLES[@]}"; do
  # 1. 校验分区元数据存在性
  echo -n "校验 $tbl 分区存在性..."
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始校验表 $tbl 的分区 dt=$dt" >> "$LOG_FILE"
  
  all_partitions=$($HIVE_CMD -S -e "use $HIVE_DB; show partitions $tbl;" 2>> "$LOG_FILE")
  if ! echo "$all_partitions" | grep -x "dt=$dt" > /dev/null; then
    error_msg="表 $tbl 缺少分区分区 dt=$dt"
    echo -e "\nERROR: $error_msg"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $error_msg" >> "$LOG_FILE"
    exit 1
  else
    echo "✅"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 表 $tbl 分区 dt=$dt 存在" >> "$LOG_FILE"
  fi

  # 2. 校验HDFS路径及数据文件
  echo -n "校验 $tbl 分区数据量..."
  hdfs_path="/user/hive/warehouse/$HIVE_DB.db/$tbl/dt=$dt"
  
  # 先通过Hive确认路径存在
  if ! $HIVE_CMD -S -e "use $HIVE_DB; dfs -test -d '$hdfs_path';" 2>> "$LOG_FILE"; then
    error_msg="分区路径不存在：$hdfs_path"
    echo -e "\nERROR: $error_msg"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $error_msg" >> "$LOG_FILE"
    exit 1
  fi
  
  # 改用本地hdfs命令统计文件（避免Hive内管道符问题）
  # 排除_SUCCESS、_tmp临时文件，统计有效数据文件
  hdfs_file_count=$(hdfs dfs -ls "$hdfs_path" 2>> "$LOG_FILE" | grep -v -E '_SUCCESS|_tmp|part-r-' | wc -l)
  hdfs_file_count=${hdfs_file_count:-0}
  
  if [ "$hdfs_file_count" -eq 0 ]; then
    error_msg="分区 $dt 无有效数据文件（路径：$hdfs_path）"
    echo -e "\nERROR: $error_msg"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $error_msg" >> "$LOG_FILE"
    exit 1
  else
    echo "✅（$hdfs_file_count 个文件）"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 表 $tbl 分区数据文件数：$hdfs_file_count" >> "$LOG_FILE"
  fi
done

echo "=== 所有分区校验通过 ==="
echo "[$(date +'%Y-%m-%d %H:%M:%S')] === 所有分区校验通过 ===" >> "$LOG_FILE"
