from airflow.hooks.base import BaseHook
import requests
import json

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    headers = api.extra_dejson.get('headers')
    if not headers:
        raise ValueError("Headers not found in the connection's extra field")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return json.dumps(response.json()['chart']['result'][0])
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching stock prices: {e}")