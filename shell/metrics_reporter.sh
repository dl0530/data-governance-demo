#!/bin/bash
set -euo pipefail

# 入参：指标名 指标值 业务标识 日期
METRIC_NAME=$1
METRIC_VALUE=$2
BUSINESS=$3
DATE=$4

# 指标文件路径
METRICS_FILE="/opt/bigdata/prometheus-ecosystem/node_exporter/textfile_collector/data_warehouse_metrics.prom"

# 写入指标（仅写一行，避免重复）
echo "${METRIC_NAME}{business=\"${BUSINESS}\",date=\"${DATE}\"} ${METRIC_VALUE}" >> "${METRICS_FILE}"

# 去重（关键）
sort -u "${METRICS_FILE}" -o "${METRICS_FILE}"

echo "指标上报成功：${METRIC_NAME} ${BUSINESS} ${DATE} → ${METRIC_VALUE}"
