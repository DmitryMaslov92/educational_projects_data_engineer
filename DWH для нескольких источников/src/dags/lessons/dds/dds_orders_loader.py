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
    object_id: str
    object_value: str
    update_ts: datetime
 
 
class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
 
    def list_orders(self, user_threshold: int, limit: int) -> List[OrdersObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    
    def get_rest_id(self, restaurant_id) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_restaurants
                 WHERE restaurant_id = %(restaurant_id)s;
                 """, {
                     "restaurant_id":restaurant_id
                 }
            )
            rest_id = cur.fetchone()
        return rest_id
    
    def get_ts_id(self, timestamp) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_timestamps
                 WHERE ts = %(timestamp)s;
                 """, {
                     "timestamp":timestamp
                 }
            )
            ts_id = cur.fetchone()
        return ts_id
    
    def get_user_id(self, user_id) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_users
                 WHERE user_id = %(user_id)s;
                 """, {
                     "user_id":user_id
                 }
            )
            user_id = cur.fetchone()
        return user_id
    
 
class OrdersDestRepository:
 
    def insert_orders(self, conn: Connection, user_id, restaurant_id, timestamp_id, order_key, order_status) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id  = EXCLUDED.timestamp_id,
                        order_key  = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status;
                """,
                {   
                    "user_id": user_id,
                    "restaurant_id": restaurant_id,
                    "timestamp_id": timestamp_id,
                    "order_key": order_key,
                    "order_status": order_status
                },
            )
 
class OrdersLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 2000 
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = OrdersOriginRepository(pg)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
 
    def load_orders(self):
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
            self.log.info(f"Found {len(load_queue)} orders to load.")
 
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            # Сохраняем объекты в базу dwh.
            for prod in load_queue:
                rest_id = self.origin.get_rest_id(str2json(prod[2])["restaurant"]['id'])[0]
                tup_ts_id = self.origin.get_ts_id(str2json(prod[2])['update_ts'])
                user_id = self.origin.get_user_id(str2json(prod[2])['user']['id'])[0]
                order_key = prod[1]
                order_status = str2json(prod[2])['final_status']
                if tup_ts_id is not None:
                    ts_id = tup_ts_id[0]
                    self.dds.insert_orders(conn, user_id, rest_id, ts_id, order_key, order_status)
 
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
 
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")