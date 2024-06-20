from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
import logging
import pandas as pd
import os
import sys

log = logging.getLogger(__name__)

pg_conn_id = 'pg_conn'
vertica_conn_id = 'vertica_conn'


def upload_data(
        table_name: str,
        dt_column: str,
        path_to_csv: str = '/data'
):    
    log.info(f'Начинается загрузка для таблицы {table_name}')

    vert_conn = VerticaHook(vertica_conn_id).get_conn()
    pg_conn = PostgresHook(pg_conn_id).get_conn()


    try:
        vert_cur = vert_conn.cursor()
        sql = f"""
                SELECT coalesce(max({dt_column}), '1900-01-01 00:00:00') 
                FROM STV230533__STAGING.{table_name};
            """
        vert_cur.execute(sql)
        last_datetime = vert_cur.fetchone()[0]

        log.info(f"Последняя запись в таблице STV230533__STAGING.{table_name} сделана {last_datetime}")

    except Exception as e:
        log.info("Error:", e)
        sys.exit()

    log.info(f"Выгрузка новых данных из источника...")

    try:
        pg_sql = f"SELECT * FROM public.{table_name} WHERE {dt_column} > '{last_datetime}';"
        df = pd.read_sql(pg_sql, pg_conn)

        if df.shape[0]==0:
            log.info(f"Новых данных не обнаружено.")
            return
        else:  
            with open(f"{path_to_csv}/{table_name}.csv", "w") as file:
                df.to_csv(file,index=False)

            log.info(f"Данные выгружены...")

    except Exception as e:
        log.info("Error:", e)
        sys.exit()

    log.info(f"Загрузка новых данных в таблицу STV230533__STAGING.{table_name}...")

    try:
        sql_columns = str(tuple(df.columns)).replace("'","")
        vert_sql = f"""
                COPY STV230533__STAGING.{table_name}{sql_columns} 
                FROM LOCAL '{path_to_csv}/{table_name}.csv'
                DELIMITER ','
                REJECTED DATA AS TABLE STV230533__STAGING.{table_name}_rej;
            """
        vert_cur.execute(vert_sql)
        log.info(f"Загрузка новых данных в таблицу STV230533__STAGING.{table_name} завершена.")
        os.remove(f'{path_to_csv}/{table_name}.csv')
    
    except Exception as e:
        log.info("Error:", e)
        sys.exit()

args = {
    "owner": "funkyabe",
    'retries': 3
}

with DAG(
    dag_id = 'upload_data',
    default_args=args,
    schedule_interval='@daily',
    start_date=pendulum.parse('2022-10-01'),
    end_date=pendulum.parse('2022-11-02'),
    catchup = True,
    tags=['final', 'upload_data', 'origin_to_stg', 'postgres']
) as dag:  
    start_task = DummyOperator(task_id='start')

    staging_ddl = SQLExecuteQueryOperator(
        task_id = 'staging_ddl',
        conn_id = vertica_conn_id,
        database = 'Vertica',
        sql = 'sql/staging_ddl.sql'
    )

    load_currencies = PythonOperator(
        task_id = 'load_currencies',
        python_callable = upload_data,
        op_kwargs={'table_name':'currencies', 'dt_column':'date_update'},
        dag=dag
    )

    load_transactions = PythonOperator(
        task_id = 'load_transactions',
        python_callable = upload_data,
        op_kwargs={'table_name':'transactions', 'dt_column':'transaction_dt'},
        dag=dag
    )

    end_task = DummyOperator(task_id='end')

    start_task >> staging_ddl >> [load_currencies, load_transactions] >> end_task


