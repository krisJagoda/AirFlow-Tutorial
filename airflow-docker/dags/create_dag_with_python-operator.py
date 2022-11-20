from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

default_args = {
    'owner': 'krystianj',
    'retries': 5,
    'retry_delay': timedelta(seconds=120)
}
local_tz = pendulum.timezone('Europe/Warsaw')


def great(name, age):
    print(f"Hello World! My name is {name}, "
          f"and I am {age} years old!")


with DAG(
        default_args=default_args,
        dag_id='my_first_dag_with_python_operator_v01',
        description='My first DAG with Python Operator',
        start_date=datetime(2022, 11, 20, tzinfo=local_tz),
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='great',
        python_callable=great,
        op_kwargs={'name': 'Krystian', 'age': 33}
    ),
    task2 = PythonOperator(
        task_id='say_hi',
        python_callable=great,
        op_kwargs={'name': 'Adam', 'age': 32}
    )

    task1 >> task2
