from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from datetime import datetime
import requests

from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv, BUCKET_NAME

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

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )

    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids='get_formatted_csv') }}}}",
            conn_id='minio'
        ),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(schema='public')
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').extra_dejson["endpoint_url"].split('//')[1],
        }
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw

stock_market()