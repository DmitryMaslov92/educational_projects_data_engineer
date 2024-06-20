from logging import Logger
from typing import List
from datetime import datetime
from lessons.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
 
 
 
class CourierObj(BaseModel):
    id: int
    courier_id: str
    name: str
 
 
class CouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
 
    def list_couriers(self, threshold: int, limit: int) -> List[CourierObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, courier_id, name
                    FROM stg.deliverysystem_couriers
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
 
 
class CourierDestRepository:
 
    def insert_courier(self, conn: Connection, courier_id, name) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name;
                """,
                {   
                    "courier_id": courier_id,
                    "name": name,
                },
            )
 
 
class CouriersLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000 
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = CouriersOriginRepository(pg)
        self.dds = CourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
 
    def load_couriers(self):
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
            load_queue = self.origin.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} objects to load.")
 
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            # Сохраняем объекты в базу dwh.
            for cour in load_queue:
                self.dds.insert_courier(conn, cour[1], cour[2])
 
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
 
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")