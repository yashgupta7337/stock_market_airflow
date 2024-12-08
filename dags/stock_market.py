from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
import requests

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market', '1st_dag', '$META']
)

def stock_market(): # dag id = stock_market
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('meta_stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    is_api_available()

stock_market()