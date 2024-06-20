from logging import Logger
from typing import List
from datetime import datetime 
from lessons.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel
 
 
 
class RestaurantObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime
 
 
class RestaurantOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
 
    def list_restaurants(self, user_threshold: int, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
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

 
class ProductsDestRepository:
 
    def insert_products(self, conn: Connection, restaurant_id, product_id, product_name, product_price, active_from, active_to) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        product_id = EXCLUDED.product_id,
                        product_name  = EXCLUDED.product_name,
                        product_price  = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {   
                    "restaurant_id": restaurant_id,
                    "product_id": product_id,
                    "product_name": product_name,
                    "product_price": product_price,
                    "active_from": active_from,
                    "active_to": active_to
                },
            )
 
 
class ProductLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Юзеров мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = RestaurantOriginRepository(pg)
        self.dds = ProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
 
    def load_products(self):
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
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
 
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            # Сохраняем объекты в базу dwh.
            for prod in load_queue:
                menu = str2json(prod[2])["menu"]

                active_to = datetime(2099, 12, 31)

                for i in range(len(menu)):
                    self.dds.insert_products(conn, prod[0], menu[i]["_id"], menu[i]["name"], float(menu[i]["price"]), prod[3], active_to)
 
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
 
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")