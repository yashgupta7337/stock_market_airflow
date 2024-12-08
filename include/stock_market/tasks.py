from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
import requests
import json
from minio import Minio
from io import BytesIO


BUCKET_NAME = 'stock-market'


def _get_minio_client():
    minio = BaseHook.get_connection('minio')

    try:
        client = Minio(
            endpoint=minio.extra_dejson["endpoint_url"].split('//')[1],
            access_key=minio.login,
            secret_key=minio.password,
            secure=False
        )
        return client
    except Exception as e:
        print(f"Error initializing Minio client: {e}")
        raise

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
    client = _get_minio_client()

    try:
        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            print(f"Bucket {BUCKET_NAME} created successfully.")
        

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
            bucket_name=BUCKET_NAME,
            object_name=f'{symbol}/prices.json',
            data=BytesIO(data),
            length=len(data)
        )
        print(f"Uploaded to {objw.bucket_name}/{symbol}/prices.json")
        return f'{objw.bucket_name}/{symbol}'
    except Exception as e:
        print(f"Error storing stock prices in Minio: {e}")
        raise


def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    csv_file = next((obj.object_name for obj in objects if obj.object_name.endswith('.csv')), None)
    if csv_file:
        return csv_file
    raise AirflowNotFoundException('The csv file does not exist')