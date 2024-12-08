from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
import requests

from include.stock_market.tasks import _get_stock_prices, _store_prices

SYMBOL = 'META'

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market', '1st_dag']
)

def stock_market(): # dag_id = stock_market
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        headers = api.extra_dejson.get('headers')
        if not headers:
            raise ValueError("Headers not found in the connection's extra field")

        try:
            response = requests.get(url, headers=headers)
            condition = response.json()['finance']['result'] is None
            print(f"Returning URL: {url}")
            return PokeReturnValue(is_done=condition, xcom_value=url)
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching stock prices: {e}")

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={
            'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}',
            'symbol': SYMBOL
        }
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={
            'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'
        }
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARG': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices

stock_market()