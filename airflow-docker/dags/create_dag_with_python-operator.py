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


def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello World! My name is {first_name} {last_name}, "
          f"and I am {age} years old!")


def get_name(ti):
    ti.xcom_push(key='first_name', value='Kristian')
    ti.xcom_push(key='last_name', value='J')


def get_age(ti):
    ti.xcom_push(key='age', value=33)


with DAG(
        default_args=default_args,
        dag_id='my_first_dag_with_python_operator_v02',
        description='My first DAG with Python Operator',
        start_date=datetime(2022, 11, 20, tzinfo=local_tz),
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    ),
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    ),
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    task2 >> task3 >> task1

# Note maximum XCOM size in memory is 48KB!
