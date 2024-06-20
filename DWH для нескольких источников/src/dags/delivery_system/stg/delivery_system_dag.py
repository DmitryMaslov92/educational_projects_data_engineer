import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from delivery_system.stg.courier_loader import CourierLoader
from delivery_system.stg.restaurants_loader import RestaurantLoader
from delivery_system.stg.deliveries_loader import DeliveryLoader



log = logging.getLogger(__name__)

NICKNAME = 'funkyabe'
COHORT = '12'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['project', 'delivery_system', 'stg', 'origin'],
    is_paused_upon_creation=False
)
def origin_to_stg_deliverysystem_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_couriers():
        log.info(">>> Load Couriers from Delivery system")
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()

    @task()
    def load_restaurants():
        log.info(">>> Load Restaurants from Delivery system")
        restaurant_loader = RestaurantLoader(dwh_pg_connect, log)
        restaurant_loader.load_restaurants()

    @task()
    def load_deliveries():
        log.info(">>> Load Deliveries from Delivery system")
        delivery_loader = DeliveryLoader(dwh_pg_connect, log)
        delivery_loader.load_deliveries()

    couriers = load_couriers()
    restaurants = load_restaurants()
    deliveries = load_deliveries()

    couriers
    restaurants
    deliveries

stg_delivery_system_dag = origin_to_stg_deliverysystem_dag()