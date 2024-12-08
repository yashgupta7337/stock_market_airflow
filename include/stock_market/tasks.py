from airflow.hooks.base import BaseHook
import requests
import json
from minio import Minio
from io import BytesIO

def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    headers = api.extra_dejson.get('headers')
    if not headers:
        raise ValueError("Headers not found in the connection's extra field")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print(len(response.json()['chart']['result'][0]))
        return json.dumps(response.json()['chart']['result'][0])
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching stock prices: {e}")


def _store_prices(stock):
    minio = BaseHook.get_connection('minio')

    try:
        client = Minio(
            endpoint=minio.extra_dejson["endpoint_url"].split('//')[1],
            access_key=minio.login,
            secret_key=minio.password,
            secure=False
        )
    except Exception as e:
        print(f"Error initializing Minio client: {e}")
        raise

    bucket_name = 'stock-market'

    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket {bucket_name} created successfully.")
        

        print(f"Received stock data: {stock}")
        try:
            stock = json.loads(stock)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON input for stock: {e}")
            raise

        symbol = stock.get('meta', {}).get('symbol')
        if not symbol:
            raise ValueError("Missing 'symbol' in stock metadata")

        data = json.dumps(stock, ensure_ascii=False).encode('utf8')
        
        objw = client.put_object(
            bucket_name=bucket_name,
            object_name=f'{symbol}/prices.json',
            data=BytesIO(data),
            length=len(data)
        )
        print(f"Uploaded to {objw.bucket_name}/{symbol}/prices.json")
        return f'{objw.bucket_name}/{symbol}'
    except Exception as e:
        print(f"Error storing stock prices in Minio: {e}")
        raise