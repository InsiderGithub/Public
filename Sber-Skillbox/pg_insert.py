import pandas as pd
import json
from sqlalchemy import create_engine
import re
import logging
import os
from airflow import DAG
import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator

def pg_insert():
    list_files = os.listdir("/mnt/c/Users/serii/FINAL_PROJECT_SKILLBOX/dataset_sberautopodpiska")
    for c in list_files:
        with open(f"/mnt/c/Users/serii/FINAL_PROJECT_SKILLBOX/dataset_sberautopodpiska/{c}", 'rb') as file:
            a = json.load(file)
        if a.get(list(a)[0]) == []:
            logging.info(f'Файл {c} пустой')
            continue
        else:
            pass
        df_json = pd.DataFrame(a.get(list(a)[0]))
        connection = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/airflow_metadata')
        list_words = re.split('_', c)
        if list_words[1] == 'sessions':
            df_json.to_sql('test_sessions', con=connection, if_exists='append', index=False)
        elif list_words[1] == 'hits':
            df_json.to_sql('test_hits', con=connection, if_exists='append', index=False)
        else:
            logging.info(f'Ошибочный файл {c}')
    return logging.info('ОТРАБОТАЛ!')

with DAG(
    dag_id='pg_insert',
    start_date=datetime.datetime(2022, 12, 12),
    schedule_interval=timedelta(minutes=1),             # minutes, days
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='pg_insert_task',
        python_callable=pg_insert,
        dag=dag
    )
    task