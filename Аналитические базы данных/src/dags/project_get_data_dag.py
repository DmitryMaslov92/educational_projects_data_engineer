from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import logging
import pandas as pd
import boto3

from botocore.exceptions import BotoCoreError, ClientError


AWS_ACCESS_KEY_ID = Variable.get('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = Variable.get('aws_secret_access_key')

def fetch_s3_file(bucket: str, key: str):
        
    try:
        session = boto3.session.Session()
        
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        s3_client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=f'/data/{key}'
        ) 

        df = pd.read_csv(f"/data/{key}")
        df['user_id_from'] = pd.array(df['user_id_from'], dtype="int64")
        logging.info(df.head(5))
        with open(f"/data/{key}", "w") as file:
            df.drop_duplicates().to_csv(file,index=False)

    except (BotoCoreError, ClientError) as e:
        logging.error(f"An error occurred while fetching S3 file: {e}")


with DAG(
    'project_dag_get_data',
    schedule_interval=None, 
    start_date=pendulum.parse('2022-07-13'),
    tags=['project5', 'get_data', 'origin_dwh', 's3']
) as dag:
    
    dl_group_logs = PythonOperator(
                task_id = f'fetch_group_log',
                python_callable=fetch_s3_file,
                op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
                dag=dag
            )

    dl_group_logs