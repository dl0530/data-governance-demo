import pandas as pd
import matplotlib.pyplot as plt
import os

# 统一设置图表样式
plt.rcParams['figure.figsize'] = (12, 10)  # 增大画布尺寸
plt.rcParams['font.size'] = 10
plt.rcParams['grid.alpha'] = 0.7
plt.rcParams['grid.linestyle'] = '--'

# 数据路径
csv_path = "/opt/donglin/data-governance-demo/show/csv_data/"

# ===================== 1. 订单指标分析 =====================
df_order = pd.read_csv(csv_path + "dws_order_stats_di.csv", names=['dt', 'total_order_count', 'total_order_amount'])
plt.subplot(2,2,1)
plt.plot(df_order['dt'], df_order['total_order_amount'], color='#2563EB', linewidth=3, marker='o', markersize=6)
plt.title('Total Order Amount Trend')
plt.xlabel('Date')
plt.ylabel('Amount (Yuan)')
plt.xticks(rotation=30, ha='right')  # 旋转日期标签并右对齐
plt.grid(True)

# ===================== 2. 用户指标分析 =====================
df_user = pd.read_csv(csv_path + "dws_user_stats_di.csv", names=['dt', 'new_user_count', 'total_user_count'])
plt.subplot(2,2,2)
plt.bar(df_user['dt'], df_user['new_user_count'], color='#F59E0B', alpha=0.8, width=0.6)
plt.title('New User Count Trend')
plt.xlabel('Date')
plt.ylabel('New User Count')
plt.xticks(rotation=30, ha='right')
plt.grid(axis='y')

# ===================== 3. 商品指标分析 =====================
df_product = pd.read_csv(csv_path + "dws_product_stats_di.csv", names=['dt', 'total_product_count', 'avg_product_price', 'high_price_rate', 'new_product_count'])
plt.subplot(2,2,3)
ax1 = plt.gca()
ax1.bar(df_product['dt'], df_product['new_product_count'], color='#10B981', alpha=0.8, width=0.6)
ax1.set_xlabel('Date')
ax1.set_ylabel('New Product Count', color='#10B981')
ax1.tick_params(axis='y', labelcolor='#10B981')
ax1.grid(axis='y')

ax2 = ax1.twinx()
ax2.plot(df_product['dt'], df_product['avg_product_price'], color='#6366F1', linewidth=3, marker='o')
ax2.set_ylabel('Avg Product Price', color='#6366F1')
ax2.tick_params(axis='y', labelcolor='#6366F1')

plt.title('New Product & Avg Price Trend')
plt.xticks(rotation=30, ha='right')

# ===================== 4. 用户转化指标分析 =====================
df_convert = pd.read_csv(csv_path + "dws_user_order_basic_di.csv", names=['dt', 'order_user_count', 'order_user_rate'])
plt.subplot(2,2,4)
plt.plot(df_convert['dt'], df_convert['order_user_count'], color='#EC4899', linewidth=3, marker='s', markersize=6)
plt.title('Order User Count Trend')
plt.xlabel('Date')
plt.ylabel('User Count')
plt.xticks(rotation=30, ha='right')
plt.grid(True)

# 自适应布局
plt.tight_layout()
plt.show()
