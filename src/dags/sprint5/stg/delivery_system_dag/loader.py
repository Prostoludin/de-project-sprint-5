from lib import PgConnect
from lib.dict_util import json2str
from logging import Logger
from psycopg import Connection
from sprint5.stg import EtlSetting, StgEtlSettingsRepository
from typing import Any, Dict, List
import requests
import json

class CourierSaver:

    def save_object(self, conn: Connection, id: str, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(object_id, object_value)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "id": id,
                    "val": str_val
                }
            )

class CourierReader:
    url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
    headers = {
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        "X-Nickname": "Prostoludin",
        "X-Cohort": str(26)
        }
    sort_field = 'id'
    sort_direction = 'asc'
            
    def get_couriers(self, offset: int, limit: int) -> str:
        r = requests.get(self.url+f"couriers?sort_field={self.sort_field}&sort_direction={self.sort_direction}&limit={limit}&offset={offset}", headers=self.headers)
        docs = json.loads(r.content)
        return docs

class CourierLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 50

    WF_KEY = "delivery_system_couriers_origin_to_stg_workflow"
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"

    def __init__(self, collection_loader: CourierReader, pg_dest: PgConnect, pg_saver: CourierSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id = 0,
                    workflow_key = self.WF_KEY,
                    workflow_settings = {
                        self.LAST_LOADED_OFFSET_KEY: 0
                    }
                )

            last_loaded_offset_str = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY]
            self.log.info(f"starting to load from last checkpoint: {last_loaded_offset_str}")

            load_queue = self.collection_loader.get_couriers(last_loaded_offset_str, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection")
            if not load_queue:
                self.log.info("Quitting")
                return 0
            
            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing couriers.")

            wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = last_loaded_offset_str + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

class DeliverySaver:

    def save_object(self, conn: Connection, id: str, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(object_id, object_value)
                    VALUES (%(id)s, %(val)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "id": id,
                    "val": str_val
                }
            )

class DeliveryReader:
    url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
    headers = {
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        "X-Nickname": "Prostoludin",
        "X-Cohort": str(26)
        }
    sort_field = 'id'
    sort_direction = 'asc'
            
    def get_deliveries(self, offset: int, limit: int) -> str:
        r = requests.get(self.url+f"deliveries?restaurant_id=&from={'2022-01-01 00:00:00'}&to={'2500-01-01 00:00:00'}&sort_field={self.sort_field}&sort_direction={self.sort_direction}&limit={limit}&offset={offset}", headers=self.headers)
        docs = json.loads(r.content)
        return docs

class DeliveryLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 50

    WF_KEY = "delivery_system_deliveries_origin_to_stg_workflow"
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"

    def __init__(self, collection_loader: DeliveryReader, pg_dest: PgConnect, pg_saver: DeliverySaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id = 0,
                    workflow_key = self.WF_KEY,
                    workflow_settings = {
                        self.LAST_LOADED_OFFSET_KEY: 0
                    }
                )

            last_loaded_offset_str = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY]
            self.log.info(f"starting to load from last checkpoint: {last_loaded_offset_str}")

            load_queue = self.collection_loader.get_deliveries(last_loaded_offset_str, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from deliveries collection")
            if not load_queue:
                self.log.info("Quitting")
                return 0
            
            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["delivery_id"]), d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing deliveries.")

            wf_setting.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = last_loaded_offset_str + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)