from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

import yfinance as yf
import json
import requests
import ast
import pandas as pd

from datetime import datetime, timedelta
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

def _check_bucket(client):
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
    _check_bucket(client)
    
    symbol_dict = {
    "symbols": symbols
    }
    symbols_json = json.dumps(symbol_dict, indent=4).encode('utf-8')
    
    try:
        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f'symbol/symbols.json',
            data=BytesIO(symbols_json),
            length=len(symbols_json)
        )
        print(f"Successfully uploaded {f'symbol/symbols.json'} to MiniO")
    except Exception as e:
        print(f"Error uploading to MiniO: {e}")
        
def _get_symbols_from_MiniO():
    client = _get_minio_client()
    _check_bucket(client)
    object_name = "symbol/symbols.json"

    try:
        data = client.get_object(BUCKET_NAME, object_name)
        symbols_json = json.loads(data.read().decode('utf-8'))
        symbols_str = symbols_json.get('symbols', "[]")        
        symbols_list = ast.literal_eval(symbols_str)  
        
        return symbols_list
    except Exception as e:
        print(f"Error retrieving symbols from MiniO: {e}")
        return None

def _get_stock_prices(symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start_date, end=end_date)
    
    df.reset_index(inplace=True)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    
    result = {
        'meta': {'symbol': symbol},
        'prices': df.to_dict(orient='records')
    }
    data = json.dumps(result)
    _store_prices(data)

def _store_prices(stock):
    client = _get_minio_client()
    _check_bucket(client)
    
    stock_data = json.loads(stock)
    symbol = stock_data['meta']['symbol']
    data = json.dumps(stock_data, ensure_ascii=False).encode('utf-8')
    client_object = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{client_object.bucket_name}/{symbol}'

def _format_prices(symbol):
    client = _get_minio_client()
    _check_bucket(client)
    
    json_file_path = f"{symbol}/prices.json"
    
    try:
        data = client.get_object(BUCKET_NAME, json_file_path)
        prices_json = json.loads(data.read().decode('utf-8'))
        
        prices_df = pd.DataFrame(prices_json['prices'])
        selected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        prices_df = prices_df[selected_columns]
        
        csv_data = prices_df.to_csv(index=False).encode('utf-8')
        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f"{symbol}/formatted_prices.csv",
            data=BytesIO(csv_data),
            length=len(csv_data)
        )
        print(f"{symbol}/formatted_prices.csv uploaded successfully.")
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for object in objects:
        if object.object_name.endswith('.csv'):
            return object.object_name
    raise AirflowNotFoundException('The csv file does not exist')