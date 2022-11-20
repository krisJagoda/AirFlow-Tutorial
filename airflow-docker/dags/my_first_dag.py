from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

default_args = {
    'owner': 'kjagoda',
    'retries': 3,
    'retry_delay': timedelta(seconds=120)
}
local_tz = pendulum.timezone('Europe/Warsaw')

with DAG(
    dag_id='my_first_dag',
    description='This is my first dag that I have written',
    default_args=default_args,
    start_date=datetime(2022, 11, 16, tzinfo=local_tz),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo Hello World!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo This is the second task which needs to be executed."
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo Third task will execute after the second one completes successfully."
    )
    # first method to create task dependencies:
    # task1.set_downstream(task2)
    # task2.set_downstream(task3)

    # second method to create task dependencies:
    # task1 >> task2
    # task1 >> task3

    # third method to create task dependencies:
    task1 >> [task2, task3]
