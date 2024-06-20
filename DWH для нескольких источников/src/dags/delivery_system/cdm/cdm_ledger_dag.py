import logging

import pendulum
from airflow.decorators import dag, task
from delivery_system.cdm.ledger_loader import LegerLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='10/15 * * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), 
    catchup=False,  
    tags=['project', 'cdm', 'courier_ledger'], 
    is_paused_upon_creation=True  
)
def project_cdm_dag():

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="update_mart")
    def update_mart():
        mart_loader = LegerLoader(dwh_pg_connect, log)
        mart_loader.update_mart() 


    mart_dict = update_mart()

    mart_dict  


cdm_dag = project_cdm_dag()
