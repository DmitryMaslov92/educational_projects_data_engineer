import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention_weekly',
        default_args=args,
        catchup=True,
        start_date=datetime.today() - timedelta(weeks=6),
        schedule_interval='0 3 * * 1',
) as dag:

    mart_update = PostgresOperator(
        task_id = 'mart_update',
        postgres_conn_id=postgres_conn_id,
        sql = "sql/mart.f_customer_retention.sql")

mart_update

