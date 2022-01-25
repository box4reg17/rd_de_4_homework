

import pandas as pd
import psycopg2 as pg
from datetime import datetime
import os

pg_cred = {
    'host': 'localhost',
    'port': '5432',
    'database': 'dshop',
    'user': 'pguser',
    'password': 'secret'
}

ROOT_FOLDER_PATH = os.path.join('.', 'data', 'hw_l7_from_postgresql')

def test_load():
    with pg.connect(**pg_cred) as pg_connection:
        df = pd.read_sql('select * from aisles', con=pg_connection)
        df.to_parquet('output-pandas.parquet')
        print(df)

def save_table_to_parquet(pg_connection, table, save_to):
    df = pd.read_sql(f'select * from {table}', con=pg_connection)
    file_path = os.path.join(save_to,f'{pg_cred.get("database", "table")}_{table}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
    df.to_parquet(file_path)

def get_all_db_tables(t_schema):
    with pg.connect(**pg_cred) as pg_connection:
        sql = "SELECT table_schema, table_name "
        sql += "FROM information_schema.tables "
        sql += "WHERE (table_schema = '" + t_schema + "') "
        sql += "ORDER BY table_schema, table_name;"
        db_cursor = pg_connection.cursor()
        db_cursor.execute(sql)
        list_tables = db_cursor.fetchall()
        folder_path = os.path.join(ROOT_FOLDER_PATH,datetime.now().strftime("%Y-%m-%d"))
        os.makedirs(folder_path, exist_ok=True)
        for t_name_table in list_tables:
            save_table_to_parquet(pg_connection=pg_connection,
                                    table=t_name_table[1],
                                    save_to=folder_path)
            print(f'{t_name_table[1]} saved in folder {folder_path}')

if __name__ == '__main__':
    get_all_db_tables(t_schema='public')


