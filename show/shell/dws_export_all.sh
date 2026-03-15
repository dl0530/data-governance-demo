#!/bin/bash
# 脚本功能：一键导出demo_dws库下4张核心表，生成标准逗号分隔的csv，输出到show/csv_data
# 项目根路径（按实际修改）
BASE_PATH="/opt/donglin/data-governance-demo"
# 导出数据存放路径
CSV_PATH="${BASE_PATH}/show/csv_data"
# Hive库名
HIVE_DB="demo_dws"
# ====================== 配置区 ======================
# ！！！核心注意：请根据实际需求修改下方的分区时间条件，适配不同时段数据导出，或者修改where条件
EXPORT_DT="2026-01-08"
# =================================================================

# 创建目录（防止目录不存在报错）
mkdir -p ${CSV_PATH}

echo "=============== 开始导出DWS层数据到 ${CSV_PATH} ==============="
echo "=============== 当前导出分区条件：dt >= ${EXPORT_DT} ==============="

# 1. 导出订单主题表 dws_order_stats_di
hive -e "select dt,total_order_count,total_order_amount from ${HIVE_DB}.dws_order_stats_di where dt >= '${EXPORT_DT}' order by dt;" | sed 's/[ \t]\+/,/g' > ${CSV_PATH}/dws_order_stats_di.csv

# 2. 导出用户主题表 dws_user_stats_di
hive -e "select dt,new_user_count,total_user_count from ${HIVE_DB}.dws_user_stats_di where dt >= '${EXPORT_DT}' order by dt;" | sed 's/[ \t]\+/,/g' > ${CSV_PATH}/dws_user_stats_di.csv

# 3. 导出商品主题表 dws_product_stats_di
hive -e "select dt,total_product_count,avg_product_price,high_price_rate,new_product_count from ${HIVE_DB}.dws_product_stats_di where dt >= '${EXPORT_DT}' order by dt;" | sed 's/[ \t]\+/,/g' > ${CSV_PATH}/dws_product_stats_di.csv

# 4. 导出用户转化表 dws_user_order_basic_di
hive -e "select dt,order_user_count,order_user_rate from ${HIVE_DB}.dws_user_order_basic_di where dt >= '${EXPORT_DT}' order by dt;" | sed 's/[ \t]\+/,/g' > ${CSV_PATH}/dws_user_order_basic_di.csv

echo "=============== 4张DWS表导出完成！文件列表如下 ==============="
ls -lh ${CSV_PATH}/dws_*.csv

echo "=============== 数据导出校验（文件行数） ==============="
for file in ${CSV_PATH}/dws_*.csv
do
    line_num=$(wc -l $file | awk '{print $1}')
    echo "文件: $(basename $file) , 数据行数: ${line_num} 行"
done

echo "=============== 导出结束 ==============="
