from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

python_path = Variable.get("python_path")

default_args = {
    'owner': 'brock',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 6),
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

dag = DAG('bigsquid_domo_to_snowflake',
         default_args=default_args,
         template_searchpath=[python_path],
         schedule_interval='0 8 * * *' # Every night at 2am
         )


task1 = BashOperator(
    task_id='bigsquid_domo_to_snowflake',
    bash_command='python3 {python_path}scripts/domo_to_snowflake.py'.format(python_path=python_path),
    dag=dag
)

task1


