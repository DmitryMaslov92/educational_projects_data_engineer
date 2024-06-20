import airflow
import os 

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import date, datetime

# прописываем пути
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id = "sprint7_project_dag",
    default_args=default_args,
    schedule_interval=None,
)

dt = '{{ ds }}'

# объявляем задачу с помощью SparkSubmitOperator
user_geo = SparkSubmitOperator(
                        task_id='user_geo_task',
                        dag=dag_spark,
                        application ='/scripts/m1_user_geo.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [f"{dt}",
                                            "/user/funkyabe/data/geo",
                                            "/user/funkyabe/data/geo.csv",
                                            "/user/funkyabe/analytics/users_geo_mart"
                                            ],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


zones = SparkSubmitOperator(
                        task_id='zones_task',
                        dag=dag_spark,
                        application ='connection_interests.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [f"{dt}",
                                            "/user/funkyabe/data/geo",
                                            "/user/funkyabe/data/geo.csv",
                                            "/user/funkyabe/analytics/users_geo_mart",
                                            "/user/funkyabe/analytics/zones_mart",
                                            ],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


recs = SparkSubmitOperator(
                        task_id='recommendations_task',
                        dag=dag_spark,
                        application ='connection_interests.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [f"{dt}",
                                            "/user/funkyabe/data/geo",
                                            "/user/funkyabe/data/geo.csv",
                                            "/user/funkyabe/analytics/users_geo_mart",
                                            "/user/funkyabe/analytics/recommendations_mart",
                                            ],
                        conf={
            "spark.driver.maxResultSize": "20g"
        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )


user_geo >> [zones, recs]


