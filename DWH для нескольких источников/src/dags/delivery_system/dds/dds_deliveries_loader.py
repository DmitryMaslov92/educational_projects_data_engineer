from logging import Logger
from typing import List
from datetime import datetime 
from lessons.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection


class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
 
    def list_deliveries(self, threshold: int, limit: int) -> List:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, delivery_id, courier_id, address, delivery_ts
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    
    def get_ts_id(self, timestamp) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_delivery_timestamps
                 WHERE ts = %(timestamp)s;
                 """, {
                     "timestamp":timestamp
                 }
            )
            ts_id = cur.fetchone()
        return ts_id
    
    def get_courier_id(self, courier_id) -> None:
        with self._db.client().cursor() as cur:
            cur.execute(
                 """
                 SELECT id
                 FROM dds.dm_couriers
                 WHERE courier_id = %(courier_id)s;
                 """, {
                     "courier_id":courier_id
                 }
            )
            courier_id = cur.fetchone()
        return courier_id
    
    
 
class DeliveriesDestRepository:
 
    def insert_orders(self, conn: Connection, delivery_id, courier_id, address, delivery_timestamp_id) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, courier_id, address, delivery_timestamp_id)
                    VALUES (%(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_timestamp_id)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        address = EXCLUDED.address,
                        delivery_timestamp_id = EXCLUDED.delivery_timestamp_id
                """,
                {   
                    "delivery_id": delivery_id,
                    "courier_id": courier_id,
                    "address": address,
                    "delivery_timestamp_id": delivery_timestamp_id,
                },
            )
 
class DeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 2000 
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = DeliveriesOriginRepository(pg)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
 
    def load_deliveries(self):
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
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} objects to load.")
 
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            # Сохраняем объекты в базу dwh.
            for deliv in load_queue:
                delivery_id = deliv[1]
                tup_courier_id = self.origin.get_courier_id(deliv[2])
                address = deliv[3]
                tup_ts_id = self.origin.get_ts_id(deliv[-1])
                if tup_ts_id is not None and tup_courier_id is not None:
                    ts_id = tup_ts_id[0]
                    courier_id = tup_courier_id[0]
                    self.dds.insert_orders(conn, delivery_id, courier_id, address, ts_id)
 
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
 
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")