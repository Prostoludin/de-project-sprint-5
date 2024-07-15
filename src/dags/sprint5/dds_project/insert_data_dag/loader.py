from logging import Logger
from typing import List

from sprint5.dds_project.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date, datetime, time


class Userobj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str

class UserOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_users(self, user_threshold: int) -> List[Userobj]:
        with self._db.client().cursor(row_factory=class_row(Userobj)) as cur:
            cur.execute(
                """
                    SELECT id AS id,
                    object_id AS user_id,
                    (object_value ::JSON->> 'name') AS user_name,
                    (object_value ::JSON->> 'login') AS user_login
                    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": user_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All users copy")        
        return objs
    
class UserDestRepository:
    def insert_user(self, conn: Connection, user: Userobj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.dm_users(id, user_id, user_name, user_login)
                    VALUES (%(id)s, %(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (id) DO UPDATE
                    SET user_id = EXCLUDED.user_id,
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_name;
                """,
                {
                    "id": user.id,
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                }
            )

class User_Loader:
    WF_KEY = "dm_users_to_dds_project_forkflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = UserOriginRepository(pg_conn, log)
        self.dds = UserDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_users(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_users(last_loaded)
            self.log.info(f"Found {len(load_queue)} users to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for user in load_queue:
                self.dds.insert_user(conn, user)
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class Restaurantobj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

class RestaurantOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_restaurants(self, restaurant_threshold: int) -> List[Restaurantobj]:
        with self._db.client().cursor(row_factory=class_row(Restaurantobj)) as cur:
            cur.execute(
                """
                    SELECT  id AS id,
                            object_id AS restaurant_id,
                            (object_value ::JSON->> 'name') AS restaurant_name,
                            update_ts AS active_from,
                            '2099-12-31 00:00:00.000' AS active_to
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                """, {
                    "threshold": restaurant_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All restaurants copy")        
        return objs
    
class RestaurantDestRepository:
    def insert_restaurant(self, conn: Connection, restaurant: Restaurantobj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "id": restaurant.id,
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                }
            )

class Restaurant_Loader:
    WF_KEY = "dm_restaurants_to_dds_project_forkflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = RestaurantOriginRepository(pg_conn, log)
        self.dds = RestaurantDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_restaurants(last_loaded)
            self.log.info(f"Found {len(load_queue)} restaurants to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for restaurant in load_queue:
                self.dds.insert_restaurant(conn, restaurant)
                self.log.info(f"load restaurant id {restaurant}")
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class Tsobj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time

class OrderTsOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_ts(self, ts_threshold: int) -> List[Tsobj]:
        with self._db.client().cursor(row_factory=class_row(Tsobj)) as cur:
            cur.execute(
                """
                    SELECT id,
                    CAST(object_value::JSON->>'date'AS timestamp) AS ts,
                    date_part('year', (object_value::JSON->>'date')::DATE) AS "year",
                    date_part('month', (object_value::JSON->>'date')::DATE) AS "month",
                    date_part('day', (object_value::JSON->>'date')::DATE) AS "day",
                    date(object_value::JSON->>'date') AS "date",
                    CAST(object_value::JSON->>'date' AS time) AS "time"
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s
                          AND object_value::JSON->>'final_status' IN ('CLOSED', 'CANCELLED')
                    ORDER BY id ASC
                """, {
                    "threshold": ts_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All Timestapms copy")        
        return objs
    
class OrderTsDestRepository:
    def insert_ts(self, conn: Connection, timestamp: Tsobj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.dm_order_ts(id,
                                                  ts,
                    							  "year",
                    							  "month",
                    							  "day",
                    							  "date",
                    							  "time")
                    VALUES (%(id)s,
                            %(ts)s,
                            %(year)s,
                            %(month)s,
                            %(day)s,
                            %(date)s,
                            %(time)s)
                    ON CONFLICT (id) DO UPDATE
                    SET ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time;
                """,
                {
                    "id": timestamp.id,
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "date": timestamp.date,
                    "time": timestamp.time
                }
            )

class Order_Ts_Loader:
    WF_KEY = "dm_timestamps_to_dds_project_forkflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = OrderTsOriginRepository(pg_conn, log)
        self.dds = OrderTsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_ts(last_loaded)
            self.log.info(f"Found {len(load_queue)} timestapms to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for ts in load_queue:
                self.dds.insert_ts(conn, ts)
                self.log.info(f"load ts id {ts}")
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class Orderobj(BaseModel):
    user_id: int
    restaurant_id: int
    order_ts_id: int
    order_key: str
    order_status: str
    delivery_id: int
    delivery_ts_id: int
    courier_id: int

class OrderOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_orders(self, order_threshold: int) -> List[Orderobj]:
        with self._db.client().cursor(row_factory=class_row(Orderobj)) as cur:
            cur.execute(
                """
                WITH orders AS (SELECT BTRIM(((object_value::JSON->'user')::JSON->'id')::VARCHAR, '"') AS user_id,
            				   BTRIM(((object_value::JSON->'restaurant')::JSON->'id')::VARCHAR, '"') AS restaurant_id,
        					   CAST((object_value::JSON->'update_ts')::TEXT AS timestamp) AS timestamp_id,
        					   object_id AS order_key,
        					   BTRIM((object_value::JSON->'final_status')::VARCHAR, '"') AS order_status
                			   FROM stg.ordersystem_orders),
	                 deliveries AS (SELECT  BTRIM((object_value::JSON->'order_id')::VARCHAR, '"') AS order_id,
        	 						        BTRIM((object_value::JSON->'delivery_id')::VARCHAR, '"') AS delivery_id
	 				                FROM stg.deliverysystem_deliveries) 
                SELECT  du.id AS user_id,
	                    dr.id AS restaurant_id,
	                    dt.id AS order_ts_id,
	                    order_key,
	                    order_status,
	                    dd.id AS delivery_id,
	                    dd.delivery_ts_id,
	                    dd.courier_id
                FROM orders
                JOIN dds_project.dm_restaurants dr ON dr.restaurant_id = orders.restaurant_id
                JOIN dds_project.dm_users du ON du.user_id = orders.user_id
                JOIN dds_project.dm_order_ts dt ON dt.ts = orders.timestamp_id
                JOIN deliveries d ON d.order_id = orders.order_key
                JOIN dds_project.dm_deliveries dd ON dd.delivery_id = d.delivery_id
                WHERE dt.id > %(threshold)s
                ORDER BY orders.timestamp_id ASC;
                """, {
                    "threshold": order_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All orders copy")        
        return objs

class OrderDestRepository:
    def insert_order(self, conn: Connection, order: Orderobj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.dm_orders(user_id,
                                              restaurant_id,
                                              order_ts_id,
                                              order_key,
                                              order_status,
                                              delivery_id,
                                              delivery_ts_id,
                                              courier_id)
                    VALUES (%(user_id)s,
                            %(restaurant_id)s,
                            %(order_ts_id)s,
                            %(order_key)s,
                            %(order_status)s,
                            %(delivery_id)s,
                            %(delivery_ts_id)s,
                            %(courier_id)s);
                """,
                {
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "order_ts_id": order.order_ts_id,
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "delivery_id": order.delivery_id,
                    "delivery_ts_id": order.delivery_ts_id,
                    "courier_id": order.courier_id
                }
            )

class Order_Loader:
    WF_KEY = "dm_orders_to_dds_project_forkflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts_id"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = OrderOriginRepository(pg_conn, log)
        self.dds = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_orders(last_loaded)
            self.log.info(f"Found {len(load_queue)} orders to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for order in load_queue:
                self.dds.insert_order(conn, order)
                self.log.info(f"load order_ts_id {order}")
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_ts_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class DeliveryTsOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_ts(self, ts_threshold: int) -> List[Tsobj]:
        with self._db.client().cursor(row_factory=class_row(Tsobj)) as cur:
            cur.execute(
                """
                    SELECT id,
                    	   CAST(object_value::JSON->>'delivery_ts'AS timestamp) AS ts,
                    	   date_part('year', (object_value::JSON->>'delivery_ts')::DATE) AS "year",
                    	   date_part('month', (object_value::JSON->>'delivery_ts')::DATE) AS "month",
                    	   date_part('day', (object_value::JSON->>'delivery_ts')::DATE) AS "day",
                    	   date(object_value::JSON->>'delivery_ts') AS "date",
                    	   CAST(object_value::JSON->>'delivery_ts' AS time) AS "time"
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                """, {
                    "threshold": ts_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All Timestapms copy")        
        return objs
    
class DeliveryTsDestRepository:
    def insert_ts(self, conn: Connection, timestamp: Tsobj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.dm_delivery_ts(id,
                                                  ts,
                    							  "year",
                    							  "month",
                    							  "day",
                    							  "date",
                    							  "time")
                    VALUES (%(id)s,
                            %(ts)s,
                            %(year)s,
                            %(month)s,
                            %(day)s,
                            %(date)s,
                            %(time)s)
                    ON CONFLICT (id) DO UPDATE
                    SET ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        date = EXCLUDED.date,
                        time = EXCLUDED.time;
                """,
                {
                    "id": timestamp.id,
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "date": timestamp.date,
                    "time": timestamp.time
                }
            )

class DeliveryTsLoader:
    WF_KEY = "dm_delivery_ts_to_dds_project_forkflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = DeliveryTsOriginRepository(pg_conn, log)
        self.dds = DeliveryTsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_ts(last_loaded)
            self.log.info(f"Found {len(load_queue)} timestapms to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for ts in load_queue:
                self.dds.insert_ts(conn, ts)
                self.log.info(f"load ts id {ts}")
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class CourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str

class CourierOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_couriers(self, courier_threshold: int) -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT id AS id,
                    object_id AS courier_id,
                    (object_value ::JSON->> 'name') AS courier_name
                    FROM stg.deliverysystem_couriers
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": courier_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All couriers copy")        
        return objs
    
class CourierDestRepository:
    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.dm_couriers(id, courier_id, courier_name)
                    VALUES (%(id)s, %(courier_id)s, %(courier_name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "id": courier.id,
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                }
            )

class Courier_Loader:
    WF_KEY = "dm_couriers_to_dds_priject_forkflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = CourierOriginRepository(pg_conn, log)
        self.dds = CourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_couriers(last_loaded)
            self.log.info(f"Found {len(load_queue)} couriers to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for courier in load_queue:
                self.dds.insert_courier(conn, courier)
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class Deliveryobj(BaseModel):
    delivery_id: str
    delivery_ts_id: int
    courier_id: int

class DeliveryOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_deliveries(self, delivery_threshold: int) -> List[Deliveryobj]:
        with self._db.client().cursor(row_factory=class_row(Deliveryobj)) as cur:
            cur.execute(
                """
                WITH deliveries AS (SELECT object_value,
        			        			   BTRIM((object_value::JSON -> 'delivery_id')::VARCHAR, '"') AS delivery_id,
		        			        	   (object_value::JSON -> 'delivery_ts')::TEXT::TIMESTAMP AS delivery_ts,
				        		           BTRIM((object_value::JSON -> 'courier_id')::VARCHAR, '"') AS courier_id
					                FROM stg.deliverysystem_deliveries)
                SELECT  delivery_id,
                        dt.id AS delivery_ts_id,
	                    c.id AS courier_id
                FROM deliveries del
                JOIN dds_project.dm_delivery_ts dt ON dt.ts =del.delivery_ts
                JOIN dds_project.dm_couriers c ON c.courier_id =del.courier_id
                WHERE dt.id > %(threshold)s
                ORDER BY del.delivery_ts ASC;
                """, {
                    "threshold": delivery_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All deliveries copy")        
        return objs

class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: Deliveryobj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.dm_deliveries(delivery_id,
                                              delivery_ts_id,
                                              courier_id)
                    VALUES (%(delivery_id)s,
                            %(delivery_ts_id)s,
                            %(courier_id)s);
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "delivery_ts_id": delivery.delivery_ts_id,
                    "courier_id": delivery.courier_id
                }
            )

class DeliveryLoader:
    WF_KEY = "dm_deliveries_to_dds_project_forkflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts_id"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = DeliveryOriginRepository(pg_conn, log)
        self.dds = DeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_deliveries(last_loaded)
            self.log.info(f"Found {len(load_queue)} orders to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for delivery in load_queue:
                self.dds.insert_delivery(conn, delivery)
                self.log.info(f"load delivery_ts_id {delivery}")
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.delivery_ts_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

class FctDeliveryobj(BaseModel):
    order_id: int
    delivery_id: int
    courier_id: int
    address: str
    rate: int
    order_sum: float
    order_tips: float
    ts: datetime

class FctDeliveryOriginRepository:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self._db = pg
        self.log = log
    
    def list_fcts(self, fact_threshold: datetime) -> List[FctDeliveryobj]:
        with self._db.client().cursor(row_factory=class_row(FctDeliveryobj)) as cur:
            cur.execute(
                """
                WITH del AS (SELECT	BTRIM((object_value::JSON->'courier_id')::VARCHAR(30), '"') AS courier_id,
                					BTRIM((object_value::JSON->'order_id')::VARCHAR(30), '"') AS order_id,
                					BTRIM((object_value::JSON->'delivery_id')::VARCHAR(30), '"') AS delivery_id,
                					BTRIM((object_value::JSON->'address')::VARCHAR, '"') AS address,
                					(object_value::JSON->'rate')::TEXT::INTEGER AS rate,
                					(object_value::JSON->'sum')::TEXT::NUMERIC(14,2) AS order_sum,
                					(object_value::JSON->'tip_sum')::TEXT::NUMERIC(14,2) AS order_tips
                			FROM 	stg.deliverysystem_deliveries)
                SELECT 	ord.id AS order_id,
                		ord.delivery_id,
                		ord.courier_id,
                		del.address,
                		del.rate,
                		del.order_sum,
                		del.order_tips,
                		dt.ts
                FROM dds_project.dm_orders ord
                JOIN del ON del.order_id = ord.order_key
                JOIN dds_project.dm_delivery_ts dt ON dt.id=ord.delivery_ts_id
                WHERE order_status LIKE 'CLOSED' AND dt.ts > %(threshold)s
                ORDER BY dt.ts ASC;
                """, {
                    "threshold": fact_threshold
                }
            )
            objs = cur.fetchall()
            self.log.info("All facts copy")        
        return objs
    
class FctDeliveryDestRepository:
    def insert_fact(self, conn: Connection, fact: FctDeliveryobj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds_project.fct_order_delivery (order_id,
                                                        delivery_id,
                                                        courier_id,
                                                        address,
                                                        rate,
                                                        order_sum,
                                                        order_tips)
                    VALUES (%(order_id)s,
                            %(delivery_id)s,                            
                            %(courier_id)s,
                            %(address)s,
                            %(rate)s,
                            %(order_sum)s,
                            %(order_tips)s);
                """,
                {
                    "order_id": fact.order_id,
                    "delivery_id": fact.delivery_id,
                    "courier_id": fact.courier_id,
                    "address": fact.address,                    
                    "rate": fact.rate,
                    "order_sum": fact.order_sum,
                    "order_tips": fact.order_tips
                }
            )

class FctDeliveryLoader:
    WF_KEY = "fct_order_delivery_to_dds_project_forkflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = FctDeliveryOriginRepository(pg_conn, log)
        self.dds = FctDeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fcts(self):
        with self.pg_conn.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()})
            
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            load_queue = self.stg.list_fcts(last_loaded)
            self.log.info(f"Found {len(load_queue)} facts to load")
            if not load_queue:
                self.log.info('Quitting')
                return
            
            self.log.info("Start of loading")
            for fact in load_queue:
                self.dds.insert_fact(conn, fact)
                self.log.info(f"load timestamp_id {fact}")
            self.log.info("Finish of loading")
            
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t.ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")