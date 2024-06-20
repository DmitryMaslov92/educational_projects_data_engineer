from logging import Logger
from typing import List
from datetime import datetime 
from lessons.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel
 
 
 
class OrdersObj(BaseModel):
    id: int
    event_ts: datetime
    event_value: str
 
 
class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
 
    def list_orders(self, user_threshold: int, limit: int) -> List[OrdersObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_value from stg.bonussystem_events WHERE event_type='bonus_transaction'
                    and id > %(threshold)s 
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    
    def get_order_id(self, order_id) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_orders
                 WHERE order_key = %(order_id)s;
                 """, {
                     "order_id":order_id
                 }
            )
            order_id = cur.fetchone()
        return order_id

    def get_product_id(self, product_id) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_products
                 WHERE product_id = %(product_id)s;
                 """, {
                     "product_id":product_id
                 }
            )
            product_id = cur.fetchone()
        return product_id
    
 
class FactsDestRepository:
 
    def insert_facts(self, conn: Connection, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                """,
                {   
                    "product_id": product_id,
                    "order_id": order_id,
                    "count": count,
                    "price": price,
                    "total_sum": total_sum,
                    "bonus_payment": bonus_payment,
                    "bonus_grant": bonus_grant
                },
            )
 
class FactsLoader:
    WF_KEY = "facts_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000 
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = OrdersOriginRepository(pg)
        self.dds = FactsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
 
    def load_facts(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg.connection() as conn:
 
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
 
            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
 
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                tup_order_id = self.origin.get_order_id(str2json(order[2])['order_id'])
                for prod in str2json(order[2])['product_payments']:
                    product_id = self.origin.get_product_id(prod['product_id'])
                    count = prod['quantity']
                    price = prod['price']
                    total_sum = prod['product_cost']
                    bonus_payment = prod['bonus_payment']
                    bonus_grant = prod['bonus_grant']
                    if tup_order_id is not None and product_id is not None:
                        order_id = tup_order_id[0]
                        product_id = product_id[0]
                        self.dds.insert_facts(conn, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
 
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
 
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")