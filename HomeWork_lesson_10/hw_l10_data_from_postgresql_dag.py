
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from hw_l10_data_from_postgresql_helper import get_all_db_tables as helper_get_all_db_tables


def app_hdfs(**kwargs):
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
    'hw_l10_data_from_postgresql',
    description='data from postgresql - database: dshop_bu',
    schedule_interval='@daily',
    start_date=datetime(2022,1,22,0,30),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='saving_all_tables_to_hdfs',
    python_callable=app_hdfs,
    provide_context=True,
    dag=dag
)

