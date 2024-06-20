from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import vertica_python

# понимаю, что лучше для этого создать Connection в Airflow,
# но я почему-то не смог установить модуль для работы с Vertica,
# поэтому создал одну переменную с параметрами подключения, чтобы не хардкодить
conn_info = Variable.get('vertica_conn', deserialize_json=True)


sql_query = """
                COPY STV230533__STAGING.{file}({cols}) 
                FROM LOCAL '/data/{file}.csv'
                DELIMITER ','
                REJECTED DATA AS TABLE STV230533__STAGING.{file}_rej;
            """

def load_file(filename: str, columns: str):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE STV230533__STAGING.{filename};")
        cur.execute(sql_query.format(file=filename, cols=columns))       


with DAG(
    'project_dag_load_data',
    schedule_interval=None, 
    start_date=pendulum.parse('2022-07-13'),
    tags=['project5', 'load_data', 'stg', 's3']
) as dag:
    
    load_group_log = PythonOperator(
        task_id = f'load_group_log',
        python_callable=load_file,
        op_kwargs={
            'filename': 'group_log', 
            'columns': 'group_id, user_id, user_id_from, event, datetime'
            },
        dag=dag
    )

    load_group_log
