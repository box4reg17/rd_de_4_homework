
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from hw_l7_data_from_api_helper import get_data as helper_get_data

def app(**kwargs):
    process_date = kwargs['ds']
    print(f"kwargs['ds'] - {kwargs['ds']}")
    helper_get_data(process_date)


default_args = {
    'owner': 'airflow',
    'email': ['some@email.com'],
    'email_on_failure': False,
    'retries': 0
}

dag = DAG(
    'hw_l7_data_from_api',
    description='"out of stock" from API - https://robot-dreams-de-api.herokuapp.com',
    schedule_interval='@daily',
    start_date=datetime(2021,1,22,0,30),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='saving_out_of_stock_to_json_files',
    python_callable=app,
    provide_context=True,
    dag=dag
)

