#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据治理全流程 Airflow 调度 DAG
运维说明：
1. 脚本适配 Airflow 3.0.6 Standalone 模式，基于 Python 3.9+ 
2. 核心流程：ODS同步 → 数据校验 → DWD清洗 → DWS汇总
3. 日志路径：
   - Airflow任务日志：/home/airflow/airflow/logs/data_governance_full_flow/
   - 业务执行日志：/opt/donglin/data-governance-demo/logs/
4. 故障排查优先级：权限 > 路径 > 组件依赖 > 任务状态
"""

# ====================== 系统模块导入 ======================
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag

# ====================== 核心配置（运维需关注） ======================
# 项目根目录（绝对路径，修改需同步更新所有脚本路径）
PROJECT_ROOT = "/opt/donglin/data-governance-demo"
# Airflow虚拟环境激活脚本（运维：需确认venv路径及airflow用户权限）
VENV_ACTIVATE = "/home/airflow/venv39/bin/activate"
# Airflow内置日期变量：执行日期（前一天，格式YYYY-MM-DD）
DEFAULT_DATE = "{{ ds }}"

# 环境变量配置（运维：需与服务器实际环境变量一致，避免工具找不到）
ENV_VARS = {
    # PATH：包含所有大数据工具路径，需覆盖Sqoop/Hive/Hadoop/JAVA
    "PATH": "/home/airflow/venv39/bin:/opt/bigdata/sqoop-1.4.7.bin__hadoop-2.6.0/bin:/opt/bigdata/hive-3.1.2/bin:/opt/bigdata/hadoop-3.3.6/bin:/opt/bigdata/hadoop-3.3.6/sbin:/usr/lib/jvm/java-1.8.0-openjdk/bin:/home/airflow/.local/bin:/home/airflow/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin",
    # 大数据组件根目录（运维：需与服务器实际安装路径一致）
    "HADOOP_HOME": "/opt/bigdata/hadoop-3.3.6",
    "SQOOP_HOME": "/opt/bigdata/sqoop-1.4.7.bin__hadoop-2.6.0",
    "HIVE_HOME": "/opt/bigdata/hive-3.1.2",
    "JAVA_HOME": "/usr/lib/jvm/java-1.8.0-openjdk",
    # Hadoop配置目录（运维：需确认配置文件生效）
    "HADOOP_CONF_DIR": "/opt/bigdata/hadoop-3.3.6/etc/hadoop",
    # 业务日志目录（运维：需确保airflow用户有写入权限）
    "LOG_DIR": os.path.join(PROJECT_ROOT, "logs"),
}

# DAG默认参数（运维：故障重试、超时时间可根据业务调整）
default_args = {
    'owner': 'airflow',          # 任务归属者
    'depends_on_past': False,    # 不依赖历史任务执行结果
    'email_on_failure': False,   # 故障不发邮件（生产可配置邮箱）
    'retries': 2,                # 故障重试次数（运维：网络抖动可设2-3次）
    'retry_delay': timedelta(minutes=5),  # 重试间隔（避免频繁重试）
    'execution_timeout': timedelta(minutes=60),  # 全局任务超时（覆盖所有子任务）
}

# ====================== DAG定义（核心调度逻辑） ======================
@dag(
    dag_id='data_governance_full_flow',  # DAG唯一标识（运维：需与日志目录名一致，无特殊字符）
    default_args=default_args,
    description='数据治理全流程：ODS同步 → 行数/分区校验 → DWD清洗 → DWS汇总',
    start_date=datetime(2025, 11, 5),    # 起始日期（运维：固定日期避免时区异常）
    schedule='@daily',                   # 调度频率（每天执行，等价于cron: 0 0 * * *）
    catchup=False,                       # 不补跑历史任务（运维：首次部署必设False）
    tags=['data-governance', 'ods', 'dwd', 'dws'],  # 标签（便于运维筛选DAG）
    # 运维扩展：禁用XCom避免状态表冲突（解决之前状态不一致问题）
    render_template_as_native_obj=True,
)
def full_data_flow():
    """
    数据治理全流程任务链
    依赖关系：
    ods_sync → [row_check, partition_check]（并行） → dwd_clean → dws_agg
    运维注意：
    1. 并行任务仅依赖ods_sync成功，可提升执行效率
    2. dwd_clean需等待所有校验完成，避免脏数据进入清洗环节
    """

    # ---------------------- 任务1：ODS层数据同步（MySQL→Hive） ----------------------
    # 运维说明：
    # - 脚本路径：{PROJECT_ROOT}/shell/03_sqoop_ods.sh
    # - 核心依赖：Sqoop连接MySQL正常、Hive ODS库存在
    # - 故障排查：检查MySQL密码、Hive分区路径权限
    ods_sync = BashOperator(
        task_id='ods_sync',  # 任务ID（运维：需与日志子目录名一致）
        bash_command=f'''
            # 激活Airflow虚拟环境（运维：确保venv路径正确）
            source {VENV_ACTIVATE} && 
            # 执行ODS同步脚本，传入业务日期参数
            sh {PROJECT_ROOT}/shell/03_sqoop_ods.sh {DEFAULT_DATE}
        ''',
        env=ENV_VARS,                # 注入环境变量
        do_xcom_push=False,          # 禁用XCom（避免状态表冲突）
        execution_timeout=timedelta(minutes=15),  # 单独超时配置（Sqoop同步耗时较短）
    )

    # ---------------------- 任务2：数据行数校验 ----------------------
    # 运维说明：
    # - 脚本路径：{PROJECT_ROOT}/check/04_rowcheck_batch.py
    # - 核心逻辑：校验ODS层表行数是否符合预期（非0）
    # - 故障排查：检查Python环境、Hive查询权限
    row_check = BashOperator(
        task_id='row_check',
        bash_command=f'''
            source {VENV_ACTIVATE} && 
            python3 {PROJECT_ROOT}/check/04_rowcheck_batch.py {DEFAULT_DATE}
        ''',
        env=ENV_VARS,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )

    # ---------------------- 任务3：数据分区校验 ----------------------
    # 运维说明：
    # - 脚本路径：{PROJECT_ROOT}/check/05_partition_check.sh
    # - 核心逻辑：校验ODS层表指定日期分区是否存在
    # - 故障排查：检查Hive分区语法、目录权限
    partition_check = BashOperator(
        task_id='partition_check',
        bash_command=f'''
            source {VENV_ACTIVATE} && 
            sh {PROJECT_ROOT}/check/05_partition_check.sh {DEFAULT_DATE}
        ''',
        env=ENV_VARS,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )

    # ---------------------- 任务4：DWD层数据清洗 ----------------------
    # 运维说明：
    # - 脚本路径：{PROJECT_ROOT}/shell/06_run_dwd.sh
    # - 核心逻辑：ODS层→DWD层清洗（去重、格式标准化）
    # - 故障排查：Hive MapReduce资源、SQL语法、临时目录权限
    dwd_clean = BashOperator(
        task_id='dwd_clean',
        bash_command=f'''
            source {VENV_ACTIVATE} && 
            sh {PROJECT_ROOT}/shell/06_run_dwd.sh {DEFAULT_DATE}
        ''',
        env=ENV_VARS,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),  # 清洗耗时较长，延长超时
        trigger_rule='all_success',  # 仅所有上游任务成功才执行（显式配置更稳定）
    )

    # ---------------------- 任务5：DWS层数据汇总 ----------------------
    # 运维说明：
    # - 脚本路径：{PROJECT_ROOT}/shell/07_run_dws.sh
    # - 核心逻辑：DWD层→DWS层汇总（用户/商品/订单维度统计）
    # - 故障排查：SQL脚本绝对路径、Hive汇总语法、日志目录权限
    dws_agg = BashOperator(
        task_id='dws_agg',
        bash_command=f'''
            source {VENV_ACTIVATE} && 
            sh {PROJECT_ROOT}/shell/07_run_dws.sh {DEFAULT_DATE}
        ''',
        env=ENV_VARS,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=20),  # 汇总耗时配置
        trigger_rule='all_success',  # 新增：显式配置
    )

    # ---------------------- 任务依赖关系（运维：调整需同步更新注释） ----------------------
    # 逻辑：ODS同步完成 → 并行执行两种校验 → 校验通过 → DWD清洗 → DWS汇总
    ods_sync >> [row_check, partition_check] >> dwd_clean >> dws_agg

# ====================== DAG实例化（运维：不可删除） ======================
# 实例化DAG对象，Airflow通过此语句识别调度任务
dag = full_data_flow()

"""
运维故障排查速查：
1. 任务状态异常（running/failed不匹配）：
   - 执行：pkill -f airflow && nohup airflow standalone > ~/airflow.log 2>&1 &
   - 清除任务状态：Airflow Web → 任务 → Clear → 勾选下游任务
2. 日志目录带引号：
   - 删除异常目录：rm -rf /home/airflow/airflow/logs/'dag_id=data_governance_full_flow'
   - 重启Airflow后重新触发
3. 权限拒绝（Permission denied）：
   - 统一目录所有者：chown -R airflow:airflow /opt/donglin/data-governance-demo
   - 临时关闭SELinux：setenforce 0（生产建议配置上下文）
4. 工具找不到（sqoop/hive: command not found）：
   - 检查ENV_VARS中的PATH是否包含工具路径
   - 手动执行env命令对比Airflow环境变量
"""
