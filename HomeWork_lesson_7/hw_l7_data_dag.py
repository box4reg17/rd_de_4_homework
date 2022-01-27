
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from hw_l7_data_from_api_helper import get_data as helper_get_data
from hw_l7_data_from_postgresql_helper import get_all_db_tables as helper_get_all_db_tables


def app_api(**kwargs):
    process_date = kwargs['ds']
    print(f"kwargs['ds'] - {kwargs['ds']}")
    helper_get_data(process_date)

def app_postgresql(**kwargs):
    process_date = kwargs['ds']
    print(f"kwargs['ds'] - {kwargs['ds']}")
    helper_get_all_db_tables(t_schema='public')

default_args = {
    'owner': 'airflow',
    'email': ['some@email.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    'hw_l7_data_from_api_and_postgresql',
    description='data from api (https://robot-dreams-de-api.herokuapp.com) and postgresql (dshop)',
    schedule_interval='@daily',
    start_date=datetime(2021,1,22,0,30),
    default_args=default_args
)

task_api = PythonOperator(
    task_id='out_of_stock_from_API',
    python_callable=app_api,
    provide_context=True,
    dag=dag
)

task_postgresql = PythonOperator(
    task_id='dshop_saving_all_tables_to_parquet_files',
    python_callable=app_postgresql,
    provide_context=True,
    dag=dag
)

