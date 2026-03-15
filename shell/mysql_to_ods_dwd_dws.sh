#!/bin/bash
set -euo pipefail

# ===================== 核心配置（日期逻辑正确） =====================
# 业务日期：传参则用传参值，不传则默认前一天
BUSINESS_DATE=${1:-$(date -d '-1 day' +%F)}
# 项目路径
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(dirname "${SCRIPT_DIR}")
# 日志路径
LOG_DIR="${PROJECT_ROOT}/logs/full_flow/${BUSINESS_DATE}"
MAIN_LOG="${LOG_DIR}/full_flow_main.log"
# 监控指标配置
METRICS_FILE="/opt/bigdata/prometheus-ecosystem/node_exporter/textfile_collector/data_warehouse_metrics.prom"
REPORTER_SCRIPT="${PROJECT_ROOT}/shell/metrics_reporter.sh"
BUSINESS_TAG="full_data_governance_flow"
# 同步的表列表
TABLES=("u_user" "o_order" "o_order_detail" "p_product")

# ===================== 工具函数（必须先定义，后调用！） =====================
# 1. 日志函数（最先定义）
log_info() {
    local msg="$1"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo -e "[$timestamp] [INFO] $msg" | tee -a "${MAIN_LOG}"
}

log_error() {
    local msg="$1"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo -e "[$timestamp] [ERROR] $msg" | tee -a "${MAIN_LOG}"
    exit 1
}

# 2. 指标清理函数
clean_old_metrics() {
    log_info "清理${BUSINESS_DATE}的旧监控指标..."
    # 删除所有该日期的同步行数指标
    sed -i "/data_warehouse_sync_rows.*date=\"${BUSINESS_DATE}\"/d" "${METRICS_FILE}"
    sed -i "/data_warehouse_sync_total_rows.*date=\"${BUSINESS_DATE}\"/d" "${METRICS_FILE}"
    sed -i "/data_warehouse_cost_time_seconds.*date=\"${BUSINESS_DATE}\"/d" "${METRICS_FILE}"
    sed -i "/data_warehouse_full_flow.*date=\"${BUSINESS_DATE}\"/d" "${METRICS_FILE}"
    sed -i "/data_warehouse_step_status.*date=\"${BUSINESS_DATE}\"/d" "${METRICS_FILE}"
    sed -i "/data_warehouse_success_rate.*date=\"${BUSINESS_DATE}\"/d" "${METRICS_FILE}"
}

# 3. 指标上报函数
report_metric() {
    local metric_name=$1
    local metric_value=$2
    local business=$3
    # 先删除旧行，再写入新行（彻底避免重复/0值）
    sed -i "/^${metric_name}{business=\"${business}\",date=\"${BUSINESS_DATE}\"}/d" "${METRICS_FILE}"
    echo "${metric_name}{business=\"${business}\",date=\"${BUSINESS_DATE}\"} ${metric_value}" >> "${METRICS_FILE}"
    log_info "上报指标：${metric_name} ${business} ${BUSINESS_DATE} → ${metric_value}"
}

# 4. 执行步骤函数
run_step() {
    local step_name="$1"
    local exec_cmd="$2"
    local script_path="$3"
    local log_file="${LOG_DIR}/$4"
    shift 4
    local script_args="$*"
    local step_tag=$(echo "${step_name}" | awk -F'-' '{print $1}')

    log_info "-------------------------------------------------"
    log_info "开始执行：${step_name}"
    log_info "执行命令：${exec_cmd} ${script_path} ${script_args}"
    
    # 执行步骤并记录日志
    if [[ "${script_path}" =~ "03_sqoop_ods.sh" ]]; then
        log_info "说明：该脚本为静默执行，完整日志在：${PROJECT_ROOT}/logs/sqoop_ods_${BUSINESS_DATE}.log"
        ${exec_cmd} "${script_path}" ${script_args}
    else
        log_info "步骤日志：${log_file}"
        ${exec_cmd} "${script_path}" ${script_args} > "${log_file}" 2>&1
    fi

    # 步骤执行成功，更新状态
    if [ $? -eq 0 ]; then
        log_info "${step_name} ✅ 执行成功"
        for i in "${!STEP_STATUS[@]}"; do
            if [[ "${STEP_STATUS[$i]}" =~ "${step_tag}:" ]]; then
                STEP_STATUS[$i]="${step_tag}:1"
                break
            fi
        done
    else
        log_error "${step_name} ❌ 执行失败！"
    fi
}

# ===================== 初始化（函数定义完成后，再执行逻辑） =====================
# 创建日志目录
mkdir -p "${LOG_DIR}" || log_error "创建日志目录失败 ${LOG_DIR}"
# 清理旧指标（关键：先删再写，避免重复）
clean_old_metrics
# 记录全流程开始时间
START_TIME=$(date +%s)
# 步骤状态初始化（0=失败，1=成功）
STEP_STATUS=("ods_sync:0" "row_check:0" "partition_check:0" "dwd_clean:0" "dws_agg:0")

log_info "=== 数据治理全流程启动 - 业务日期：${BUSINESS_DATE} ==="
log_info "项目根目录：${PROJECT_ROOT}"
log_info "日志归档目录：${LOG_DIR}"
log_info "总控日志文件：${MAIN_LOG}"

# ===================== 执行核心步骤 =====================
# 1. ODS层同步
run_step "ODS层-全量数据同步" sh "${PROJECT_ROOT}/shell/03_sqoop_ods.sh" "ods_sync.log" "${BUSINESS_DATE}"
# 2. 行数校验
run_step "数据校验-ODS表行数非空校验" python3 "${PROJECT_ROOT}/check/04_rowcheck_batch.py" "row_check.log" "${BUSINESS_DATE}"
# 3. 分区校验
run_step "数据校验-ODS表分区存在性校验" sh "${PROJECT_ROOT}/check/05_partition_check.sh" "partition_check.log" "${BUSINESS_DATE}"
# 4. DWD层清洗
run_step "DWD层-数据清洗(去重/标准化/过滤脏数据)" sh "${PROJECT_ROOT}/shell/06_run_dwd.sh" "dwd_clean.log" "${BUSINESS_DATE}"
# 5. DWS层聚合
run_step "DWS层-业务指标汇总计算" sh "${PROJECT_ROOT}/shell/07_run_dws.sh" "dws_agg.log" "${BUSINESS_DATE}"

# ===================== 上报监控指标（同步完成后，无0值） =====================
log_info "开始上报${BUSINESS_DATE}的监控指标（所有同步已完成）..."

# 1. 上报各表同步行数（查询Hive实际数据）
TOTAL_ROWS=0
for TABLE in "${TABLES[@]}"; do
    BUSINESS="sqoop_ods_full_sync_${TABLE}"
    # 同步完成后查询，结果为正确值
    SYNC_ROWS=$(hive -e "use demo_ods; select count(*) from ods_${TABLE} where dt = '${BUSINESS_DATE}';" | grep -v "OK" | grep -v "Time taken")
    # 空值兜底（仅当真的无数据时为0）
    SYNC_ROWS=${SYNC_ROWS:-0}
    # 累加总行数
    TOTAL_ROWS=$((TOTAL_ROWS + SYNC_ROWS))
    # 上报单表行数（内置去重逻辑）
    report_metric "data_warehouse_sync_rows" "${SYNC_ROWS}" "${BUSINESS}"
done

# 2. 上报总行数
report_metric "data_warehouse_sync_total_rows" "${TOTAL_ROWS}" "sqoop_ods_full_sync"
# 3. 上报同步成功率（总行数>0则为1，否则为0）
SUCCESS_RATE=$([ ${TOTAL_ROWS} -gt 0 ] && echo 1 || echo 0)
report_metric "data_warehouse_success_rate" "${SUCCESS_RATE}" "sqoop_ods_full_sync"
# 4. 上报同步耗时
COST_TIME=$(( $(date +%s) - START_TIME ))
report_metric "data_warehouse_cost_time_seconds" "${COST_TIME}" "sqoop_ods_full_sync"

# 5. 上报全流程指标
report_metric "data_warehouse_full_flow_success" 1 "${BUSINESS_TAG}"
report_metric "data_warehouse_full_flow_cost_time" "${COST_TIME}" "${BUSINESS_TAG}"

# 6. 上报各步骤状态
for step in "${STEP_STATUS[@]}"; do
    step_tag=$(echo "${step}" | cut -d':' -f1)
    step_status=$(echo "${step}" | cut -d':' -f2)
    report_metric "data_warehouse_step_status" "${step_status}" "${BUSINESS_TAG}_${step_tag}"
done

# ===================== 收尾 =====================
# 指标文件全局去重（最终兜底）
sort -u "${METRICS_FILE}" -o "${METRICS_FILE}"
# 重启Node Exporter加载新指标
sudo systemctl restart node_exporter || log_info "Node Exporter重启失败（非关键错误）"

log_info "-------------------------------------------------"
log_info "=== 数据治理全流程 ✅ 全部执行完成 - 业务日期：${BUSINESS_DATE} ==="
log_info "ODS同步详细日志：${PROJECT_ROOT}/logs/sqoop_ods_${BUSINESS_DATE}.log"
log_info "其他步骤日志归档至：${LOG_DIR}"
log_info "监控指标已上报至：${METRICS_FILE}"
