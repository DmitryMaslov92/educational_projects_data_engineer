import logging

import pendulum
from airflow.decorators import dag, task
from lessons.dds.dds_users_loader import UsersLoader
from lessons.dds.dds_orders_loader import OrdersLoader
from lessons.dds.dds_products_loader import ProductLoader
from lessons.dds.dds_restaurants_loader import RestaurantLoader
from lessons.dds.dds_timestamps_loader import TsLoader
from lessons.dds.dds_facts_loader import FactsLoader

from delivery_system.dds.dds_couriers_loader import CouriersLoader
from delivery_system.dds.dds_delivery_timestamps_loader import TsLoader
from delivery_system.dds.dds_deliveries_loader import DeliveriesLoader
from delivery_system.dds.dds_fct_deliv_loader import FactsLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='2/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 5, 18, 4, 30, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def project_dds_full_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    # origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UsersLoader(dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductLoader(dwh_pg_connect, log)
        rest_loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        ts_loader = OrdersLoader(dwh_pg_connect, log)
        ts_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.
    
    @task(task_id="timestamps_load")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        ts_loader = TsLoader(dwh_pg_connect, log)
        ts_loader.load_ts()  # Вызываем функцию, которая перельет данные.

    @task(task_id="facts_load")
    def load_facts():
        # создаем экземпляр класса, в котором реализована логика.
        facts_loader = FactsLoader(dwh_pg_connect, log)
        facts_loader.load_facts()  # Вызываем функцию, которая перельет данные.



    # Проект
    @task(task_id="couriers_load")
    def load_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = CouriersLoader(dwh_pg_connect, log)
        rest_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="delivery_timestamps_load")
    def load_deliv_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TsLoader(dwh_pg_connect, log)
        rest_loader.load_ts()  # Вызываем функцию, которая перельет данные.

    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        ts_loader = DeliveriesLoader(dwh_pg_connect, log)
        ts_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    @task(task_id="delivery_facts_load")
    def load_deliv_facts():
        # создаем экземпляр класса, в котором реализована логика.
        ts_loader = FactsLoader(dwh_pg_connect, log)
        ts_loader.load_facts()  # Вызываем функцию, которая перельет данные.
        

    users_dict = load_users()
    ts_dict = load_timestamps()
    restaurants_dict = load_restaurants()
    couriers_dict = load_couriers()
    deliv_ts_dict = load_deliv_timestamps()

    order_dict = load_orders()
    product_dict = load_products()
    deliv_dict = load_deliveries()

    facts_dict = load_facts()
    deliv_facts_dict = load_deliv_facts()



    restaurants_dict >> product_dict
    restaurants_dict >> order_dict
    ts_dict >> order_dict
    couriers_dict >> deliv_dict
    deliv_ts_dict >> deliv_dict
    users_dict >> order_dict
    product_dict >> facts_dict
    order_dict >> facts_dict
    order_dict >> deliv_facts_dict
    deliv_dict >> deliv_facts_dict

    


dds_dag = project_dds_full_dag()
