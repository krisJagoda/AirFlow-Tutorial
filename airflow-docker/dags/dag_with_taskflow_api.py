from datetime import timedelta, datetime
from typing import Dict

from airflow.decorators import dag, task
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

default_args = {
    'owner': 'krystianj',
    'retries': 5,
    'retry_delay': timedelta(seconds=120)
}
local_tz = pendulum.timezone('Europe/Warsaw')


@dag(
    dag_id='dag_with_taskflow_api_v01',
    default_args=default_args,
    description='My first DAG with Taskflow API',
    start_date=datetime(2022, 11, 20, tzinfo=local_tz),
    schedule_interval='@daily'
)
def hello_world():
    pass

    @task
    def get_name() -> Dict:
        return {
            'first_name': 'Kristian',
            'last_name': 'J'
        }

    @task
    def get_age():
        return 33

    @task
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name} "
              f"and I am {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet(
        first_name=name_dict['first_name'],
        last_name=name_dict['last_name'],
        age=age
    )


hello_world_dag = hello_world()

