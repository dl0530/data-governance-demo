#!/bin/bash
# ==========================================
# 🚀 Flink 全量表自动同步脚本 (新手友好版)
# ==========================================
# 功能：读取配置的表列表，动态生成 SQL，并提交 Flink 任务
# 用法：./flink_all_tables_sync.sh
# ==========================================

set -e # 遇到错误立即停止

echo "🔍 [1/4] 正在检查并初始化环境..."

# --- ⚙️ 基础配置区 (请根据实际服务器路径修改) ---
export HADOOP_USER_NAME=donglin  # HDFS 操作用户名

# Hadoop 安装路径 (如果环境变量已存在则使用，否则使用默认值)
HADOOP_HOME="${HADOOP_HOME:-/opt/bigdata/hadoop-3.3.6}"
# Flink 安装路径
FLINK_HOME="${FLINK_HOME:-/opt/bigdata/flink-1.17.1}"

# 验证路径是否存在
if [ ! -d "$HADOOP_HOME" ] || [ ! -d "$FLINK_HOME" ]; then
    echo "❌ 错误：找不到 Hadoop 或 Flink 目录，请检查脚本顶部的路径配置！"
    echo "   HADOOP_HOME: $HADOOP_HOME"
    echo "   FLINK_HOME: $FLINK_HOME"
    exit 1
fi

# --- 🔑 核心依赖注入 (解决 ClassNotFound 的关键) ---
echo "⚡ 正在注入 Hadoop 依赖到 Flink 环境..."
if [ -z "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
fi
# 将 Hadoop 类路径追加到 Flink 类路径，确保 TaskManager 能找到 HDFS
export FLINK_CLASSPATH="${FLINK_CLASSPATH}:${HADOOP_CLASSPATH}"

# --- 📂 项目路径配置 ---
BASE_DIR="$(cd "$(dirname "$0")" && pwd)" # 自动获取当前脚本所在目录
LOG_DIR="${BASE_DIR}/logs"
SQL_TEMPLATE="${BASE_DIR}/all_table_sync_template.sql"

# 创建日志目录
mkdir -p "$LOG_DIR"

# --- 🌐 HDFS 存储配置 ---
HDFS_HOST="bigdata"       # NameNode 主机名
HDFS_PORT="8020"          # NameNode 端口 (通常是 8020 或 9000)
HDFS_WAREHOUSE="/user/hive/warehouse/demo_ods.db" # 数仓根目录

echo "✅ 环境初始化完成."
echo "   📁 日志目录: $LOG_DIR"
echo "   📝 SQL 模板: $SQL_TEMPLATE"

# --- 📋 表同步配置列表 (⚠️ 新增表请在此处添加) ---
# 格式说明："MySQL表名|Kafka Topic|目标表名 | 字段定义"
# 字段定义格式：列名:类型:JSON中的键名 (多个字段用逗号分隔)
declare -a TABLE_CONFIGS=(
    # 示例表：u_user
    # 含义：从 canal_topic 消费 u_user 表，写入 ods_u_user
    # 字段映射：user_id(INT) <- 'user_id', username(STRING) <- 'username' ...
    "u_user|canal_topic|ods_u_user|user_id:INT:user_id,username:STRING:username,register_time:STRING:register_time,update_time:STRING:update_time"
    
    # 👇 在这里添加新表，复制上一行修改即可
    # "u_order|canal_topic|ods_u_order|order_id:BIGINT:order_id,amount:DECIMAL(10,2):amount"
)

# --- 🛠️ 核心处理函数 ---
process_table() {
    local config_line="$1"
    
    # 解析配置字符串
    IFS='|' read -r mysql_table kafka_topic hive_table field_defs <<< "$config_line"
    
    # 去除空格
    mysql_table=$(echo "$mysql_table" | xargs)
    kafka_topic=$(echo "$kafka_topic" | xargs)
    hive_table=$(echo "$hive_table" | xargs)
    
    echo ""
    echo "=========================================="
    echo "🚀 [2/4] 正在处理表: $mysql_table -> $hive_table"
    echo "=========================================="

    # 1. 动态构建 SQL 的 Schema 部分 和 Select 部分
    local schema_fields=""
    local select_logic=""
    
    # 解析字段定义 (支持 DECIMAL(10,2) 等复杂类型)
    IFS=',' read -ra FIELDS <<< "$field_defs"
    for item in "${FIELDS[@]}"; do
        # 提取 JSON 键 (最后一个冒号后)
        json_key=$(echo "$item" | rev | cut -d':' -f1 | rev)
        # 提取 类型 (中间部分)
        col_type=$(echo "$item" | rev | cut -d':' -f2 | rev)
        # 提取 列名 (第一个冒号前)
        col_name=$(echo "$item" | cut -d':' -f1)
        
        if [ -n "$col_name" ]; then
            # 构建 CREATE TABLE 的字段部分
            [ -n "$schema_fields" ] && schema_fields="${schema_fields}, "
            schema_fields="${schema_fields}${col_name} ${col_type}"
            
            # 构建 SELECT 的转换逻辑
            local raw_expr="data[1]['${json_key}']"
            local final_expr=""
            
            # 根据类型添加 CAST 转换
            if [[ "$col_type" == "INT" ]]; then 
                final_expr="CAST(${raw_expr} AS INT)"
            elif [[ "$col_type" == BIGINT* ]]; then 
                final_expr="CAST(${raw_expr} AS BIGINT)"
            elif [[ "$col_type" == DECIMAL* ]]; then 
                final_expr="CAST(${raw_expr} AS ${col_type})"
            else 
                final_expr="${raw_expr}"
            fi
            
            [ -n "$select_logic" ] && select_logic="${select_logic}, "
            select_logic="${select_logic}${final_expr} AS ${col_name}"
        fi
    done

    # 添加公共字段 (操作类型、时间、分区)
    schema_fields="${schema_fields}, op_type STRING, op_ts STRING, dt STRING"
    select_logic="${select_logic}, op_type, FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss') AS op_ts, DATE_FORMAT(FROM_UNIXTIME(ts/1000), 'yyyy-MM-dd') AS dt"

    # 2. 生成临时 SQL 文件
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local sql_file="${LOG_DIR}/job_${mysql_table}_${timestamp}.sql"
    local log_file="${LOG_DIR}/submit_${mysql_table}_${timestamp}.log"
    
    # 使用 sed 替换模板中的占位符
    # 注意：路径中包含 /，所以 sed 使用 | 作为分隔符
    sed -e "s|__KAFKA_TOPIC__|${kafka_topic}|g" \
        -e "s|__KAFKA_SERVER__|bigdata:9092|g" \
        -e "s|__SCHEMA_FIELDS__|${schema_fields}|g" \
        -e "s|__HDFS_PATH__|hdfs://${HDFS_HOST}:${HDFS_PORT}${HDFS_WAREHOUSE}/${hive_table}|g" \
        -e "s|__SELECT_LOGIC__|${select_logic}|g" \
        -e "s|__MYSQL_TABLE__|${mysql_table}|g" \
        "$SQL_TEMPLATE" > "$sql_file"

    echo "📝 [3/4] SQL 文件已生成: $sql_file"

    # 3. 提交任务到 Flink
    echo "⏳ [4/4] 正在提交任务到 Flink 集群..."
    
    # 使用 env 确保环境变量传递给子进程
    env HADOOP_USER_NAME=$HADOOP_USER_NAME \
        HADOOP_CLASSPATH=$HADOOP_CLASSPATH \
        FLINK_CLASSPATH=$FLINK_CLASSPATH \
        $FLINK_HOME/bin/sql-client.sh -f "$sql_file" submit 2>&1 | tee "$log_file"

    # 4. 检查结果
    if grep -q "Job ID:" "$log_file"; then
        JOB_ID=$(grep "Job ID:" "$log_file" | awk '{print $3}')
        echo "✅ ✅ ✅ 提交成功！"
        echo "🆔 Job ID: $JOB_ID"
        echo "🔗 Web UI: http://bigdata:8081/#/job/$JOB_ID"
    else
        echo "❌ ❌ ❌ 提交失败！"
        echo "👇 错误日志:"
        tail -n 20 "$log_file"
        return 1
    fi
}

# --- 🏃 主程序入口 ---
echo "📋 检测到 ${#TABLE_CONFIGS[@]} 个待同步表配置..."

for config in "${TABLE_CONFIGS[@]}"; do
    # 跳过注释行
    [[ "$config" =~ ^#.*$ ]] && continue
    # 跳过空行
    [[ -z "$config" ]] && continue
    
    process_table "$config"
    
    echo "💤 等待 3 秒，避免提交频率过高..."
    sleep 3
done

echo ""
echo "🎉 🎉 🎉 所有任务提交完毕！请前往 Flink Web UI 监控状态。"
