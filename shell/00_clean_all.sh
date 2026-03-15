#!/bin/bash
set -e

# 1. 清理MySQL库表（删除demo_oltp库及所有表）
echo "=== 清理MySQL库表 ==="
mysql -uroot -pRoot123! -e "
DROP DATABASE IF EXISTS demo_oltp;
"

# 2. 清理Hive库表（删除demo_ods/demo_dwd/demo_dws库及数据）
echo "=== 清理Hive库表 ==="
hive -e "
DROP DATABASE IF EXISTS demo_ods CASCADE;
DROP DATABASE IF EXISTS demo_dwd CASCADE;
DROP DATABASE IF EXISTS demo_dws CASCADE;
"

# 3. 清理HDFS上的Hive数据（避免残留文件）
echo "=== 清理HDFS数据 ==="
hdfs dfs -rm -r -f /user/hive/warehouse/demo_ods.db
hdfs dfs -rm -r -f /user/hive/warehouse/demo_dwd.db
hdfs dfs -rm -r -f /user/hive/warehouse/demo_dws.db

# 4. 清理增量同步时间记录（重新开始）
rm -f shell/last_sync_time.txt

echo "=== 全量清理完成 ==="
