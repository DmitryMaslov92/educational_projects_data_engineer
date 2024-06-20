from logging import Logger

from lib import PgConnect
from psycopg import Connection
from lib.dict_util import json2str
from delivery_system.stg.ds_reader import DeliverySystemReader
from lessons.stg import EtlSetting, StgEtlSettingsRepository

NICKNAME = 'funkyabe'
COHORT = '12'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'

class RestaurantRepository:

    def insert_entry(self, conn: Connection, obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_restaurants(restaurant_id, name)
                    VALUES (%(restaurant_id)s, %(name)s)
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET name = EXCLUDED.name;
                """,
                {
                    "restaurant_id": obj["_id"],
                    "name": obj["name"],
                    
                },
            )

class RestaurantLoader:

    WF_KEY = "restaurant_origin_to_stg_workflow"
    LAST_LOADED_KEY = "last_loaded_number"
    LOAD_LIMIT = 500

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliverySystemReader(log, NICKNAME, COHORT, API_KEY, BASE_URL)
        self.stg = RestaurantRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):

        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_KEY: 0})

            offset = wf_setting.workflow_settings[self.LAST_LOADED_KEY]  

            while offset < self.LOAD_LIMIT:

                load_queue = self.origin.get_restaurants(offset=offset)
                list_len = len(load_queue)

                self.log.info(f"Found {list_len} objects to load.")
                if list_len == 0:
                    self.log.info("Quitting.")
                    break


                # Сохраняем объекты в базу dwh.
                for obj in load_queue:
                    self.stg.insert_entry(conn, obj)

                offset += list_len

            wf_setting.workflow_settings[self.LAST_LOADED_KEY] = offset
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_KEY]}")

