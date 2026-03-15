import sys
import random
import os
from datetime import datetime, timedelta
import pymysql

# ====================== 配置与路径 ======================
db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "Root123!",
    "db": "demo_oltp",
    "port": 3306,
    "charset": "utf8mb4"
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


# ====================== 工具函数 ======================
def write_log(log_file, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")


def random_date(end_date, days_before=365):
    start_date = end_date - timedelta(days=days_before)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return (start_date + timedelta(days=random_number_of_days)).strftime("%Y-%m-%d %H:%M:%S")


def random_update_time(create_time_str, end_date):
    create_time = datetime.strptime(create_time_str, "%Y-%m-%d %H:%M:%S")
    max_days = (end_date - create_time).days
    if max_days <= 0:
        return create_time_str
    random_days = random.randint(1, max_days)
    update_time = create_time + timedelta(days=random_days)
    update_time = update_time.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59)
    )
    return update_time.strftime("%Y-%m-%d %H:%M:%S")


# ====================== 主逻辑 ======================
def main():
    if len(sys.argv) == 2:
        dt = sys.argv[1]
        try:
            datetime.strptime(dt, "%Y-%m-%d")
        except ValueError:
            print("日期格式错误，请使用 yyyy-MM-dd（例如：2025-11-03）")
            sys.exit(1)
    else:
        dt = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    log_file = os.path.join(LOG_DIR, f"gen_data_{dt}.log")
    write_log(log_file, f"=== 开始生成模拟业务数据（目标日期：{dt}） ===")
    print(f"生成数据的目标日期：{dt}")
    print(f"日志路径：{log_file}")

    end_date = datetime.strptime(dt, "%Y-%m-%d")

    try:
        conn = pymysql.connect(**db_config, autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        write_log(log_file, "数据库连接成功")
        print("数据库连接成功")
    except Exception as e:
        error_msg = f"数据库连接失败：{str(e)}"
        write_log(log_file, error_msg)
        print(error_msg)
        return

    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        write_log(log_file, "已临时禁用外键约束")
        print("已临时禁用外键约束")

        tables = ["o_order_detail", "o_order", "p_product", "u_user"]
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            write_log(log_file, f"已清空表：{table}")
            print(f"已清空表：{table}")

        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        write_log(log_file, "已恢复外键约束")
        print("已恢复外键约束")

        # 1. 生成用户数据
        user_count = 1000
        users = []
        update_ratio = 0.1
        for i in range(1, user_count + 1):
            username = f"user_{i:04d}"
            register_time = random_date(end_date)
            if random.random() < update_ratio:
                update_time = random_update_time(register_time, end_date)
            else:
                update_time = register_time
            users.append((i, username, register_time, update_time))
        
        cursor.executemany(
            "INSERT INTO u_user (user_id, username, register_time, update_time) VALUES (%s, %s, %s, %s)",
            users
        )
        write_log(log_file, f"生成用户数据：{user_count} 条（其中约{int(update_ratio*100)}%被更新过）")
        print(f"生成用户数据：{user_count} 条（其中约{int(update_ratio*100)}%被更新过）")

        # 2. 生成商品数据
        product_count = 500
        products = []
        update_ratio = 0.15
        for i in range(1, product_count + 1):
            product_name = f"product_{i:03d}"
            price = round(random.uniform(10, 1000), 2)
            create_time = random_date(end_date)
            if random.random() < update_ratio:
                update_time = random_update_time(create_time, end_date)
            else:
                update_time = create_time
            products.append((i, product_name, price, create_time, update_time))
        
        cursor.executemany(
            "INSERT INTO p_product (product_id, product_name, price, create_time, update_time) VALUES (%s, %s, %s, %s, %s)",
            products
        )
        write_log(log_file, f"生成商品数据：{product_count} 条（其中约{int(update_ratio*100)}%被更新过）")
        print(f"生成商品数据：{product_count} 条（其中约{int(update_ratio*100)}%被更新过）")

        # 3. 生成订单数据
        order_count = 5000
        orders = []
        update_ratio = 0.2
        for i in range(1, order_count + 1):
            user_id = random.randint(1, user_count)
            total_amount = 0.0
            order_status = random.choice([1, 2, 3])
            create_time = f"{dt} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            if random.random() < update_ratio:
                create_time_obj = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S")
                end_of_day = datetime.strptime(f"{dt} 23:59:59", "%Y-%m-%d %H:%M:%S")
                max_minutes = int((end_of_day - create_time_obj).total_seconds() / 60)
                if max_minutes > 0:
                    random_minutes = random.randint(1, max_minutes)
                    update_time = (create_time_obj + timedelta(minutes=random_minutes)).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    update_time = create_time
            else:
                update_time = create_time
            orders.append((i, user_id, total_amount, order_status, create_time, update_time))
        
        cursor.executemany(
            "INSERT INTO o_order (order_id, user_id, total_amount, order_status, create_time, update_time) VALUES (%s, %s, %s, %s, %s, %s)",
            orders
        )
        write_log(log_file, f"生成订单数据：{order_count} 条（其中约{int(update_ratio*100)}%被更新过）")
        print(f"生成订单数据：{order_count} 条（其中约{int(update_ratio*100)}%被更新过）")

        # 4. 生成订单明细数据
        detail_count = 15000
        details = []
        update_ratio = 0.05
        
        cursor.execute("SELECT product_id, price FROM p_product")
        product_prices = {row['product_id']: row['price'] for row in cursor.fetchall()}

        for i in range(1, detail_count + 1):
            order_id = random.randint(1, order_count)
            product_id = random.randint(1, product_count)
            quantity = random.randint(1, 5)
            
            cursor.execute(f"SELECT create_time FROM o_order WHERE order_id = {order_id}")
            order_create_time = cursor.fetchone()['create_time']
            
            if isinstance(order_create_time, datetime):
                create_time = order_create_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                create_time = str(order_create_time)  # 确保是字符串类型
            
            # 计算更新时间（基于字符串格式的create_time）
            if random.random() < update_ratio:
                create_time_obj = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S")  # 字符串→datetime
                end_of_day = datetime.strptime(f"{dt} 23:59:59", "%Y-%m-%d %H:%M:%S")
                max_minutes = int((end_of_day - create_time_obj).total_seconds() / 60)
                if max_minutes > 0:
                    random_minutes = random.randint(1, max_minutes)
                    update_time = (create_time_obj + timedelta(minutes=random_minutes)).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    update_time = create_time
            else:
                update_time = create_time
            
            details.append((i, order_id, product_id, quantity, create_time, update_time))

            # 批量更新订单总金额
            if i % 100 == 0:
                update_batch = []
                for d in details[-100:]:
                    _, order_id, product_id, quantity, _, _ = d
                    product_price = product_prices[product_id]
                    detail_amount = round(product_price * quantity, 2)
                    update_batch.append((detail_amount, order_id))
                cursor.executemany(
                    "UPDATE o_order SET total_amount = total_amount + %s WHERE order_id = %s",
                    update_batch
                )

        # 插入订单明细
        cursor.executemany(
            "INSERT INTO o_order_detail (detail_id, order_id, product_id, quantity, create_time, update_time) VALUES (%s, %s, %s, %s, %s, %s)",
            details
        )

        # 处理剩余明细
        remaining = detail_count % 100
        if remaining > 0:
            update_batch = []
            for d in details[-remaining:]:
                _, order_id, product_id, quantity, _, _ = d
                product_price = product_prices[product_id]
                detail_amount = round(product_price * quantity, 2)
                update_batch.append((detail_amount, order_id))
            cursor.executemany(
                "UPDATE o_order SET total_amount = total_amount + %s WHERE order_id = %s",
                update_batch
            )

        write_log(log_file, f"生成订单明细数据：{detail_count} 条（其中约{int(update_ratio*100)}%被更新过）")
        print(f"生成订单明细数据：{detail_count} 条（其中约{int(update_ratio*100)}%被更新过）")

        write_log(log_file, "所有数据生成完成！")
        print("所有数据生成完成！")

    except Exception as e:
        error_msg = f"数据生成失败：{str(e)}"
        write_log(log_file, error_msg)
        print(error_msg)
        conn.rollback()
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        cursor.close()
        conn.close()
        write_log(log_file, "数据库连接已关闭")


if __name__ == "__main__":
    main()
