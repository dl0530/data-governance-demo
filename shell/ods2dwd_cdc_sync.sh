#!/bin/bash
# ==============================================================================
# 功能：ODS→DWD CDC增量同步（调用外部SQL文件，动态传日期参数）
# 适配/sql/flink/21-24.sql文件调用
# 用法1：指定日期同步 → sh ods2dwd_cdc_sync.sh 2026-03-15
# 用法2：默认同步前一天 → sh ods2dwd_cdc_sync.sh
# SQL目录：/opt/donglin/data-governance-demo/sql/flink/
# 日志路径：/opt/donglin/data-governance-demo/logs/cdc_sync/
# ==============================================================================

# ====================== 1. 基础配置 ======================
# SQL文件根目录（根据实际路径调整）
SQL_DIR="/opt/donglin/data-governance-demo/sql/flink"
# 日志目录
LOG_DIR="/opt/donglin/data-governance-demo/logs/cdc_sync"
mkdir -p ${LOG_DIR}

# ====================== 2. 动态日期处理 ======================
SYNC_DATE=$1
if [ -z "${SYNC_DATE}" ]; then
    SYNC_DATE=$(date -d '-1 day' +'%Y-%m-%d')  # 默认前一天
fi
# 日期格式校验
if ! [[ ${SYNC_DATE} =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "错误：日期格式必须为 yyyy-MM-dd！示例：2026-03-15"
    exit 1
fi
# 日志文件命名（含日期）
LOG_FILE="${LOG_DIR}/ods2dwd_cdc_sync_${SYNC_DATE}.log"

# ====================== 3. 日志初始化 ======================
echo -e "===== $(date +'%Y-%m-%d %H:%M:%S') 开始同步【${SYNC_DATE}】数据 =====\n" > ${LOG_FILE}

# ====================== 4. 定义核心函数：执行单个SQL文件 ======================
execute_sql_file() {
    local sql_file=$1  # 传入SQL文件名
    local table_name=$2  # 表名（日志用）
    
    echo -e "----- 开始同步【${table_name}】表（SQL文件：${sql_file}） -----\n" >> ${LOG_FILE}
    
    # 步骤1：复制SQL文件到临时目录，避免修改原文件
    TMP_SQL="/tmp/$(basename ${sql_file})_${SYNC_DATE}.sql"
    cp ${SQL_DIR}/${sql_file} ${TMP_SQL}
    
    # 步骤2：动态替换占位符${SYNC_DATE}为实际日期
    sed -i "s/\${SYNC_DATE}/${SYNC_DATE}/g" ${TMP_SQL}
    
    # 步骤3：执行SQL文件
    hive -f ${TMP_SQL} >> ${LOG_FILE} 2>&1
    
    # 步骤4：结果判断
    if [ $? -eq 0 ]; then
        echo -e "----- 【${table_name}】表同步成功！ -----\n" >> ${LOG_FILE}
        rm -f ${TMP_SQL}  # 删除临时文件
    else
        echo -e "----- 【${table_name}】表同步失败！请查看日志 -----\n" >> ${LOG_FILE}
        rm -f ${TMP_SQL}
        exit 1
    fi
}

# ====================== 5. 按表执行同步（调用外部SQL文件） ======================
# 5.1 用户表同步（调用21_dwd_user_cdc_incr_sync.sql）
execute_sql_file "21_dwd_user_cdc_incr_sync.sql" "dwd_u_user"

# 5.2 商品表同步（调用22_dwd_product_cdc_incr_sync.sql）
execute_sql_file "22_dwd_product_cdc_incr_sync.sql" "dwd_p_product"

# 5.3 订单表同步（调用23_dwd_order_cdc_incr_sync.sql）
execute_sql_file "23_dwd_order_cdc_incr_sync.sql" "dwd_o_order"

# 5.4 订单明细表同步（调用24_dwd_order_detail_cdc_sync.sql）
execute_sql_file "24_dwd_order_detail_cdc_sync.sql" "dwd_o_order_detail"

# ====================== 6. 整体结果 ======================
echo -e "===== $(date +'%Y-%m-%d %H:%M:%S') 【${SYNC_DATE}】所有表同步完成！=====" >> ${LOG_FILE}
echo "✅ 同步完成！日志路径：${LOG_FILE}"
