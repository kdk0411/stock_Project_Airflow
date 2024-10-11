from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

import requests
import json

from datetime import datetime
from bs4 import BeautifulSoup
from minio import Minio
from io import BytesIO

BUCKET_NAME = 'stock-market'

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _check_buket(client):
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

def _get_most_active_stocks(url, limit=20):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    symbol_spans = soup.find_all('span', class_=lambda x: x and 'symbol' in x.split())
    symbols = []
    for span in symbol_spans[:limit]:
        symbol = span.text.strip()
        symbols.append(symbol)
        if len(symbols) == limit:
            break
    return symbols

def _store_symbols(symbols):
    client = _get_minio_client()
    _check_buket()
    today = datetime.now().strftime('%Y-%m-%d')
    
    symbol_dict = {
    "symbols": symbols
    }
    symbols_json = json.dumps(symbol_dict, indent=4).encode('utf-8')
    
    try:
        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f'symbol/{today}_symbols.json',
            data=BytesIO(symbols_json),
            length=len(symbols_json)
        )
        print(f"Successfully uploaded {f'symbol/{today}_symbols.json'} to MiniO")
    except Exception as e:
        print(f"Error uploading to MiniO: {e}")
        
def _get_symbols_from_MiniO():
    client = _get_minio_client()
    _check_buket()
    today = datetime.now().strftime('%Y-%m-%d')
    object_name = f"symbol/{today}_symbols.json"

    try:
        data = client.get_object(BUCKET_NAME, object_name)
        symbols_json = json.loads(data.read().decode('utf-8'))
        return symbols_json.get('symbols', [])
    except Exception as e:
        print(f"Error retriving symbols from MiniO: {e}")
        return None

def _get_stock_prices(api_url, symbol):
    url = f"{api_url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    client = _get_minio_client()
    _check_buket()
    
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf-8')
    client_object = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{client_object.bucket_name}/{symbol}'

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for object in objects:
        if object.object_name.endswith('.csv'):
            return object.object_name
    raise AirflowNotFoundException('The csv file does not exist')