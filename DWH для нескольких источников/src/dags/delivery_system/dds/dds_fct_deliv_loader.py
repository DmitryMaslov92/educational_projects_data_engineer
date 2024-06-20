from logging import Logger
from typing import List
from datetime import datetime 
from lessons.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
 
 
class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
 
    def list_orders(self, user_threshold: int, limit: int) -> List:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, delivery_id, order_id, rate, sum, tip_sum
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s 
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

    def get_delivery_id(self, delivery_id) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_deliveries
                 WHERE delivery_id = %(delivery_id)s;
                 """, {
                     "delivery_id":delivery_id
                 }
            )
            product_id = cur.fetchone()
        return product_id
    
 
class FactsDestRepository:
 
    def insert_facts(self, conn: Connection, delivery_id, order_id, rate, sum, tip_sum) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(delivery_id, order_id, rate, sum, tip_sum)
                    VALUES (%(delivery_id)s, %(order_id)s, %(rate)s, %(sum)s, %(tip_sum)s)
                """,
                {   
                    "delivery_id": delivery_id,
                    "order_id": order_id,
                    "rate": rate,
                    "sum": sum,
                    "tip_sum": tip_sum
                },
            )
 
class FactsLoader:
    WF_KEY = "delivery_facts_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000 
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = OriginRepository(pg)
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
            self.log.info(f"Found {len(load_queue)} objects to load.")
 
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            # Сохраняем объекты в базу dwh.
            for fact in load_queue:
                order_id = self.origin.get_order_id((fact[2]))
                deliv_id = self.origin.get_delivery_id(fact[1])
                rate = fact[3]
                sum = fact[4]
                tip_sum = fact[5]
                if order_id is not None and deliv_id is not None:
                    order_id = order_id[0]
                    deliv_id = deliv_id[0]
                    self.dds.insert_facts(conn, deliv_id, order_id, rate, sum, tip_sum)
 
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
 
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")