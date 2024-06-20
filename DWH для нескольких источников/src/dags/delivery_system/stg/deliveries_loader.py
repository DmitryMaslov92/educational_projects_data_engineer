from logging import Logger
from datetime import datetime

from lib import PgConnect
from psycopg import Connection
from lib.dict_util import json2str
from delivery_system.stg.ds_reader import DeliverySystemReader
from lessons.stg import EtlSetting, StgEtlSettingsRepository

NICKNAME = 'funkyabe'
COHORT = '12'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'
BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'

class DekiveryRepository:

    def insert_entry(self, conn: Connection, obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (order_id) DO UPDATE
                    SET order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum   
                    ;                        
                """,
                {
                    "order_id": obj["order_id"],
                    "order_ts": obj["order_ts"],
                    "delivery_id": obj["delivery_id"],
                    "courier_id": obj["courier_id"],
                    "address": obj["address"],
                    "delivery_ts": obj["delivery_ts"],
                    "rate": obj["rate"],
                    "sum": obj["sum"],
                    "tip_sum": obj["tip_sum"],                    
                },
            )

class DeliveryLoader:

    WF_KEY = "delivery_origin_to_stg_workflow"
    LAST_LOADED_KEY = "last_loaded_date"
    LOAD_LIMIT = 5000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliverySystemReader(log, NICKNAME, COHORT, API_KEY, BASE_URL)
        self.stg = DekiveryRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):

        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_KEY: datetime(1900,1,1)})

            offset = 0
            from_ts = wf_setting.workflow_settings[self.LAST_LOADED_KEY] 

            while offset < self.LOAD_LIMIT:

                load_queue = self.origin.get_deliveries(from_ts=from_ts, offset=offset)
                list_len = len(load_queue)

                self.log.info(f"Found {list_len} objects to load.")
                if list_len == 0:
                    self.log.info("Quitting.")
                    break


                # Сохраняем объекты в базу dwh.
                for obj in load_queue:
                    self.stg.insert_entry(conn, obj)

                offset += list_len
                last_ts = load_queue[-1]['order_ts'][:19]

            wf_setting.workflow_settings[self.LAST_LOADED_KEY] = last_ts
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_KEY]}")

