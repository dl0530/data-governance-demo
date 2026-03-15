import pymysql
import subprocess
import sys
import os
from datetime import datetime, timedelta

# 配置
MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "Root123!",
    "db": "demo_oltp",
    "port": 3306
}
HIVE_DB = "demo_ods"
HIVE_CMD_PATH = "/opt/bigdata/hive-3.1.2/bin/hive"
LOG_DIR = "/opt/donglin/data-governance-demo/logs/"
TABLE_MAP = {
    "u_user": "ods_u_user",
    "p_product": "ods_p_product",
    "o_order": "ods_o_order",
    "o_order_detail": "ods_o_order_detail"
}
HIVE_TIMEOUT = 180  # 延长超时时间


def write_log(log_file, message):
    """写入写入日志（包含时间戳）
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def get_mysql_count(table, dt, log_file):
    """获取MySQL表行数并记录日志"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        with conn.cursor() as cursor:
            if table == "o_order":
                sql = f"SELECT COUNT(*) FROM `{table}` WHERE DATE(create_time) = %s;"
                write_log(log_file, f"MySQL查询：{sql}，参数：{dt}")
                cursor.execute(sql, (dt,))
            else:
                sql = f"SELECT COUNT(*) FROM `{table}`;"
                write_log(log_file, f"MySQL查询：{sql}")
                cursor.execute(sql)
            count = cursor.fetchone()[0]
            write_log(log_file, f"MySQL表 {table} 行数：{count}")
            return count
    except Exception as e:
        write_log(log_file, f"MySQL查询失败：{str(e)}")
        raise  # 抛出异常，便于上层处理
    finally:
        if 'conn' in locals():
            conn.close()


def get_hive_count(hive_table, dt, log_file):
    """获取Hive表行数（延长超时时间）"""
    hive_cmd = f"{HIVE_CMD_PATH} -S -e \"USE {HIVE_DB}; SELECT COUNT(*) FROM {hive_table} WHERE dt='{dt}';\" 2>/dev/null"
    try:
        write_log(log_file, f"Hive查询命令：{hive_cmd}")
        out = subprocess.check_output(
            hive_cmd,
            shell=True,
            timeout=HIVE_TIMEOUT,
            stderr=subprocess.STDOUT
        ).decode().strip()
        if out and out.isdigit():
            count = int(out)
            write_log(log_file, f"Hive表 {hive_table}（dt={dt}）行数：{count}")
            return count
        else:
            write_log(log_file, f"Hive返回非数字结果：{out}")
            return 0
    except subprocess.TimeoutExpired:
        write_log(log_file, f"Hive查询超时（超过{HIVE_TIMEOUT}秒）")
        return -1
    except Exception as e:
        write_log(log_file, f"Hive查询失败：{str(e)}")
        return -1


def main():
    # 处理日期参数
    if len(sys.argv) == 2:
        dt = sys.argv[1]
        try:
            datetime.strptime(dt, "%Y-%m-%d")
        except ValueError:
            print("日期格式错误，请使用 yyyy-MM-dd")
            sys.exit(1)
    else:
        yesterday = datetime.now() - timedelta(days=1)
        dt = yesterday.strftime("%Y-%m-%d")
        print(f"未输入日期参数，默认使用昨天：{dt}")
    
    # 初始化日志
    log_file = os.path.join(LOG_DIR, f"rowcheck_{dt}.log")
    os.makedirs(LOG_DIR, exist_ok=True)
    write_log(log_file, f"=== 开始批量校验 dt={dt} ===")
    print(f"=== 开始批量校验 dt={dt} ===")
    
    for mysql_table, hive_table in TABLE_MAP.items():
        step_info = f"校验表：{mysql_table} → {hive_table}"
        print(f"\n{step_info}")
        write_log(log_file, step_info)
        
        try:
            mysql_cnt = get_mysql_count(mysql_table, dt, log_file)
            print(f"  MySQL 行数：{mysql_cnt}")
        except Exception as e:
            print(f"  MySQL 查询失败 ❌（详情见日志）")
            continue
        
        hive_cnt = get_hive_count(hive_table, dt, log_file)
        if hive_cnt == -1:
            print("  Hive 查询失败 ❌（详情见日志）")
            continue
        print(f"  Hive[{dt}] 行数：{hive_cnt}")
        
        # 计算差异率
        if mysql_cnt == 0:
            diff_rate = 0.0 if hive_cnt == 0 else 100.0
        else:
            diff_rate = abs(hive_cnt - mysql_cnt) / mysql_cnt * 100
        result = "✅" if diff_rate < 0.1 else "❌"
        diff_info = f"差异率：{diff_rate:.2f}% {result}"
        print(f"  {diff_info}")
        write_log(log_file, diff_info)
    
    write_log(log_file, "=== 批量校验完成 ===")
    print("\n=== 批量校验完成 ===")
    print(f"日志已保存至：{log_file}")


if __name__ == "__main__":
    main()
