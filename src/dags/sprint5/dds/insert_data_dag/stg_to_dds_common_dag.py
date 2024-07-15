import logging

import pendulum
from airflow.decorators import dag, task
from sprint5.dds.insert_data_dag.loader import User_Loader, Restaurant_Loader, Ts_Loader, Product_Loader, Order_Loader, Fct_Loader
from sprint5.dds.insert_data_dag.loader import DeliveryTsLoader, Courier_Loader, DeliveryLoader, FctDeliveryLoader
from lib import ConnectionBuilder
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def sprint5_from_stg_to_dds_common_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="users_load")
    def load_users():
        rest_loader = User_Loader(dwh_pg_connect, log)
        rest_loader.load_users()  
    
    @task(task_id="restaurants_load")
    def load_restaurants():
        rest_loader = Restaurant_Loader(dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="timestamps_load")
    def load_timestamps():
        rest_loader = Ts_Loader(dwh_pg_connect, log)
        rest_loader.load_timestamps() 

    @task(task_id="products_load")
    def load_products():
        rest_loader = Product_Loader(dwh_pg_connect, log)
        rest_loader.load_products()

    @task(task_id="orders_load")
    def load_orders():
        rest_loader = Order_Loader(dwh_pg_connect, log)
        rest_loader.load_orders()

    @task(task_id="sales_facts_load")
    def load_sales_facts():
        rest_loader = Fct_Loader(dwh_pg_connect, log)
        rest_loader.load_fcts()

    @task(task_id="couriers_load")
    def load_couriers():
        rest_loader = Courier_Loader(dwh_pg_connect, log)
        rest_loader.load_couriers()

    @task(task_id="delivery_ts_load")
    def load_delivery_ts():
        rest_loader = DeliveryTsLoader(dwh_pg_connect, log)
        rest_loader.load_timestamps()

    @task(task_id="deliveries_load")
    def load_deliveries():
        rest_loader = DeliveryLoader(dwh_pg_connect, log)
        rest_loader.load_deliveries()

    @task(task_id="delivery_facts_load")
    def load_delivery_facts():
        rest_loader = FctDeliveryLoader(dwh_pg_connect, log)
        rest_loader.load_fcts()    

    with TaskGroup('primary_measurements') as group1:
        user_loader = load_users()
        restaurant_loader = load_restaurants()
        ts_loader = load_timestamps()
        delivery_ts_loader = load_delivery_ts()
        courier_loader = load_couriers()

    with TaskGroup('seconary_measurements') as group2:
        product_loader = load_products()
        order_loader = load_orders()
        delivery_loader = load_deliveries()

    with TaskGroup('Facts') as group3:
        delivery_fact_loader = load_delivery_facts()
        sales_fact_loader = load_sales_facts()

    # Задаем порядок выполнения.
    group1 >> group2 >> group3

order_dds_dag = sprint5_from_stg_to_dds_common_dag()