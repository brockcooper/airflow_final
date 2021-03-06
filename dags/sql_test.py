from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks import SnowflakeHook
from airflow.operators import SnowflakeOperator

sql_path = Variable.get("sql_path")

default_args = {
    'owner': 'bigsquid',
    'depends_on_past': False,
    'start_date': datetime(2018, 8, 26),
    'email': ['bcooper@bigsquid.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('snowflake_sql_test',
         default_args=default_args,
         template_searchpath=[sql_path],
         schedule_interval='5 * 1 * *' # at 12:05AM every day
         )

task1 = SnowflakeOperator(
    dag=dag,
    task_id='direct_sql',
    sql='SELECT * FROM SERVICES_ACTUAL_BUDGET_HOURS;',
    snowflake_conn_id='bigsquid_snowflake',
    depends_on_past=False
)

task2 = SnowflakeOperator(
    dag=dag,
    task_id='sql_file',
    sql='sql/test.sql',
    snowflake_conn_id='bigsquid_snowflake',
    depends_on_past=False
)

task2 << task1
