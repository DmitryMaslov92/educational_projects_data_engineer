from logging import Logger
from typing import List
from datetime import datetime 
from lessons.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
 
 
 
class TsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def list_ts(self, user_threshold: int, limit: int) -> List:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, delivery_ts
                    FROM stg.deliverysystem_deliveries
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
 
 
class TsDestRepository:
 
    def insert_ts(self, conn: Connection, ts, year, month, day, time, date) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_delivery_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (ts) DO UPDATE
                    SET
                        year  = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date;
                """,
                {
                    "ts": ts,
                    "year": year,
                    "month": month,
                    "day": day,
                    "time": time,
                    "date": date
                },
            )
 
 
class TsLoader:
    WF_KEY = "deliv_ts_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_date"
    BATCH_LIMIT = 10000 
 
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.origin = TsOriginRepository(pg)
        self.dds = TsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log
 
    def load_ts(self):
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
            load_queue = self.origin.list_ts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} objects to load.")
 
            if not load_queue:
                self.log.info("Quitting.")
                return
 
            # Сохраняем объекты в базу dwh.
            for ts in load_queue:
                dt_ts = ts[1]
                self.dds.insert_ts(conn, dt_ts, dt_ts.year, dt_ts.month, dt_ts.day, dt_ts.timetz(), dt_ts.date())
 
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t[0] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
 
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")