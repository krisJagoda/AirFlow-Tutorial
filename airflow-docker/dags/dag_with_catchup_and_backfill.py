from datetime import datetime, timedelta
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
    dag_id='dag_with_catchup_backfill_v01',
    default_args=default_args,
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    schedule_interval='@daily',
    catchup=True
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo This is a bash operator task"
    )
