from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

default_args = {
    'owner': 'krystianj',
    'retries': 5,
    'retry_delay': timedelta(seconds=120)
}
local_tz = pendulum.timezone('Europe/Warsaw')

with DAG(
    dag_id='dag_with_cron_expression',
    default_args=default_args,
    start_date=datetime(2022, 1, 22),
    schedule_interval='0 */6 * * 2-5'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo DAG with custom cron expression"
    )
