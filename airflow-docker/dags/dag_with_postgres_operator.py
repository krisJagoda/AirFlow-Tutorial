from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

default_args = {
    'owner': 'krystianJ',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
postgres_conn_id = 'postgres_localhost'

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    start_date=datetime(2022, 1, 31, tzinfo=pendulum.timezone('Europe/Warsaw')),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='create_cat_table',
        postgres_conn_id=postgres_conn_id,
        sql='sql/cat_schema.sql'
    ),
    task2 = PostgresOperator(
        task_id='populate_cat_table',
        postgres_conn_id=postgres_conn_id,
        sql='sql/populate_cat_table.sql'
    ),
    task3 = PostgresOperator(
        task_id='truncate_cat_table',
        postgres_conn_id=postgres_conn_id,
        sql='''
            TRUNCATE TABLE cat
            ;
        '''
    )
    task1 >> task3 >> task2
