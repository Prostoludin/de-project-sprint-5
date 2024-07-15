import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from sprint5.stg.delivery_system_dag.loader import CourierReader, CourierLoader, CourierSaver
from sprint5.stg.delivery_system_dag.loader import DeliverySaver, DeliveryReader, DeliveryLoader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'stg', 'origin'],
    is_paused_upon_creation=True
)

def sprint5_stg_delivery_system_common_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = CourierSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CourierReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    courier_loader = load_couriers()

    @task()
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = DeliverySaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = DeliveryReader()

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = DeliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    delivery_loader = load_deliveries()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    [courier_loader, delivery_loader]

order_stg_dag = sprint5_stg_delivery_system_common_dag()  # noqa