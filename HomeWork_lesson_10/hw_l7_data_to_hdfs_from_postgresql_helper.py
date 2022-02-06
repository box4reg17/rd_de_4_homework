

import pandas as pd
import psycopg2 as pg
from datetime import datetime
import os
from hdfs import InsecureClient

pg_cred = {
    'host': 'localhost',
    'port': '5432',
    'database': 'dshop',
    'user': 'pguser',
    'password': 'secret'
}

ROOT_FOLDER_PATH = os.path.join('.', 'data', 'hw_l7_from_postgresql')
HDFS_ROOT_FOLDER_PATH = os.path.join('/','bronze','hw_l7_from_postgresql')

def test_load():
    with pg.connect(**pg_cred) as pg_connection:
        df = pd.read_sql('select * from aisles', con=pg_connection)
        df.to_parquet('output-pandas.parquet')
        print(df)

def save_table_to_parquet(pg_connection, table, save_to):
    df = pd.read_sql(f'select * from {table}', con=pg_connection)
    file_path = os.path.join(save_to,f'{pg_cred.get("database", "table")}_{table}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
    df.to_parquet(file_path)

def save_table_to_hdfs_csv(cursor, table, save_to):
    file_name = f'{pg_cred.get("database", "table")}_{table}_{datetime.now().strftime("%Y%m%d")}.csv' # _%H%M%S
    file_path = os.path.join(save_to, file_name)
    with open(file_path, 'w') as csv_file:
        cursor.copy_expert(f'COPY public.{table} TO STDOUT WITH HEADER CSV', csv_file)
    

def get_all_db_tables(t_schema):
    with pg.connect(**pg_cred) as pg_connection:
        sql = "SELECT table_schema, table_name "
        sql += "FROM information_schema.tables "
        sql += "WHERE (table_schema = '" + t_schema + "') "
        sql += "ORDER BY table_schema, table_name;"
        db_cursor = pg_connection.cursor()
        db_cursor.execute(sql)
        list_tables = db_cursor.fetchall()
        local_folder_path = os.path.join(ROOT_FOLDER_PATH,datetime.now().strftime("%Y/%m/%d"))
        os.makedirs(local_folder_path, exist_ok=True)
        for t_name_table in list_tables:
            save_table_to_hdfs_csv(cursor=db_cursor, table=t_name_table[1], save_to=local_folder_path)
            print(f'{t_name_table[1]} saved in folder {local_folder_path}')
        client = InsecureClient(f'http://127.0.0.1:50070/', user='user')
        # create directory in HDFS
        hdfs_path = os.path.join(HDFS_ROOT_FOLDER_PATH, datetime.now().strftime("%Y/%m/"))
        client.makedirs(hdfs_path)
        client.upload(hdfs_path, local_folder_path, overwrite=True)
        print('Successfully copied to HDFS - ', hdfs_path)

if __name__ == '__main__':
    get_all_db_tables(t_schema='public')


