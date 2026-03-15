#!/bin/bash
# ==============================================
# 数据治理全流程一键执行脚本 (部署路径: /opt/donglin/data-governance-demo/shell/)
# 执行方式：cd /opt/donglin/data-governance-demo/shell && sh mysql_to_ods_dwd_dws.sh [yyyy-MM-dd]
# 不传日期默认同步【昨天】，传日期同步指定日期
# 日志路径：/opt/donglin/data-governance-demo/logs/full_flow/[日期]/
# 适配特性：03_sqoop_ods.sh为静默执行，日志自动写入自身独立文件，本脚本不做重定向
# ==============================================
set -euo pipefail

# ====================== 1. 路径配置（无需修改，自动适配）======================
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(dirname "${SCRIPT_DIR}")
dt=${1:-$(date -d '-1 day' +%F)}
LOG_DIR="${PROJECT_ROOT}/logs/full_flow/${dt}"
mkdir -p "${LOG_DIR}" || { echo "ERROR: 创建日志目录失败 ${LOG_DIR}"; exit 1; }
MAIN_LOG="${LOG_DIR}/full_flow_main.log"

# ====================== 2. 日志函数【终端+日志文件 双输出】======================
log_info() {
    local msg="$1"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo -e "[$timestamp] [INFO] $msg" | tee -a "${MAIN_LOG}"
}

log_error() {
    local msg="$1"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo -e "[$timestamp] [ERROR] $msg" | tee -a "${MAIN_LOG}"
}

# ====================== 3. 步骤执行函数【核心修复！解除重定向死锁，完美适配你的静默脚本】======================
# 入参说明：$1=步骤名称  $2=执行命令  $3=子脚本路径  $4=步骤日志名  $5+=脚本参数
# 核心改动：对03_sqoop_ods.sh不做日志重定向，只执行+校验状态；python/sh其他脚本正常重定向日志
run_step() {
    local step_name="$1"
    local exec_cmd="$2"
    local script_path="$3"
    local log_file="${LOG_DIR}/$4"
    shift 4
    local script_args="$*"

    log_info "-------------------------------------------------"
    log_info "开始执行：${step_name}"
    log_info "执行命令：${exec_cmd} ${script_path} ${script_args}"
    
    # ========== 核心修复 ==========
    # 判断是否是ODS同步脚本，这个脚本自身写日志，直接执行不重定向，解除死锁
    if [[ "${script_path}" =~ "03_sqoop_ods.sh" ]]; then
        log_info "说明：该脚本为静默执行，完整日志在：/opt/donglin/data-governance-demo/logs/sqoop_ods_${dt}.log"
        ${exec_cmd} "${script_path}" ${script_args}
    else
        log_info "步骤日志：${log_file}"
        ${exec_cmd} "${script_path}" ${script_args} > "${log_file}" 2>&1
    fi

    # 统一校验执行状态，失败终止，成功继续
    if [ $? -eq 0 ]; then
        log_info "${step_name} ✅ 执行成功"
    else
        log_error "${step_name} ❌ 执行失败！"
        exit 1
    fi
}

# ====================== 4. 全流程执行【严格按依赖顺序】======================
log_info "=== 数据治理全流程启动 - 业务日期：${dt} ==="
log_info "项目根目录：${PROJECT_ROOT}"
log_info "日志归档目录：${LOG_DIR}"
log_info "总控日志文件：${MAIN_LOG}"

# ---------------------- 步骤1: ODS层同步 MySQL → Hive ----------------------
run_step \
    "ODS层-全量数据同步" \
    sh \
    "${PROJECT_ROOT}/shell/03_sqoop_ods.sh" \
    "ods_sync.log" \
    "${dt}"

# ---------------------- 步骤2: 数据行数校验 ----------------------
run_step \
    "数据校验-ODS表行数非空校验" \
    python3 \
    "${PROJECT_ROOT}/check/04_rowcheck_batch.py" \
    "row_check.log" \
    "${dt}"

# ---------------------- 步骤3: Hive分区校验 ----------------------
run_step \
    "数据校验-ODS表分区存在性校验" \
    sh \
    "${PROJECT_ROOT}/check/05_partition_check.sh" \
    "partition_check.log" \
    "${dt}"

# ---------------------- 步骤4: DWD层数据清洗 ----------------------
run_step \
    "DWD层-数据清洗(去重/标准化/过滤脏数据)" \
    sh \
    "${PROJECT_ROOT}/shell/06_run_dwd.sh" \
    "dwd_clean.log" \
    "${dt}"

# ---------------------- 步骤5: DWS层数据汇总 ----------------------
run_step \
    "DWS层-业务指标汇总计算" \
    sh \
    "${PROJECT_ROOT}/shell/07_run_dws.sh" \
    "dws_agg.log" \
    "${dt}"

# ====================== 5. 流程结束 ======================
log_info "-------------------------------------------------"
log_info "=== 数据治理全流程 ✅ 全部执行完成 - 业务日期：${dt} ==="
log_info "ODS同步详细日志：/opt/donglin/data-governance-demo/logs/sqoop_ods_${dt}.log"
log_info "其他步骤日志归档至：${LOG_DIR}"
