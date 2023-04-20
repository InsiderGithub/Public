import pandas as pd
import re
import json
import logging
import os
import psycopg2
from airflow import DAG
import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator

def pg_insert():
    try:
        conn = psycopg2.connect("dbname='airflow_metadata' user='postgres' host='localhost' port='5432' password='postgres'")
        cursor = conn.cursor()
        conn.autocommit = False
        logging.info('Добавляю в таблицу')
        list_files = os.listdir("/mnt/c/Users/serii/FINAL_PROJECT_SKILLBOX/dataset_sberautopodpiska")
        for c in list_files:
            with open(f"/mnt/c/Users/serii/FINAL_PROJECT_SKILLBOX/dataset_sberautopodpiska/{c}", 'rb') as file:
                a = json.load(file)
            if a.get(list(a)[0]) == []:
                logging.info(f'Файл {c} пустой')
                continue
            else:
                pass
            df = pd.DataFrame(a.get(list(a)[0]))
            tuples = [tuple(x) for x in df.to_numpy()]
            cols = '","'.join(list(df.columns))
            template = '(' + ','.join(['%s' for i in range(len(df.columns))]) + ')'
            list_words = re.split('_', c)
            if list_words[1] == 'sessions':
                query1 = f'INSERT INTO test_sessions("{cols}") VALUES %s'
                psycopg2.extras.execute_values(cursor, query1, tuples, template=template)
            elif list_words[1] == 'hits':
                query2 = f'INSERT INTO test_hits("{cols}") VALUES %s'
                psycopg2.extras.execute_values(cursor, query2, tuples, template=template)
            else:
                logging.info(f'Ошибочный файл {c}')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info("Ошибка в транзакции. Отмена всех остальных операций транзакции", error)
        conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()
            logging.info("Соединение с PostgreSQL закрыто")
    return logging.info('ОТРАБОТАЛ!')

with DAG(
    dag_id='pg_insert_ver_2',
    start_date=datetime.datetime(2022, 12, 12),
    schedule_interval=timedelta(minutes=2),             # minutes, days
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='pg_insert_task',
        python_callable=pg_insert,
        dag=dag
    )
    task

