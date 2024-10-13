from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import requests
from include.stock_market.task import _get_stock_prices, _get_symbols_from_MiniO
import attr

@attr.define
class CustomPokeReturnValue:
    is_done: bool
    xcom_value: str
    
def _is_api_available() -> CustomPokeReturnValue:
    api = BaseHook.get_connection('stock_api')
    url = f"{api.host}{api.extra_dejson['endpoint']}"
    response = requests.get(url, headers=api.extra_dejson['headers'])
    condition = response.json()['finance']['result'] is None
    return CustomPokeReturnValue(is_done=condition, xcom_value=url)

with DAG(
    dag_id='get_stock_prices',
    catchup=False,
    tags=['get_stock_prices']
) as dag:
    
    symbols = _get_symbols_from_MiniO()
    
    for symbol in symbols:
        is_api_available = PythonOperator(
            task_id=f'is_api_available_{symbol}',
            python_callable=_is_api_available,
            op_kwargs={'symbol': symbol},
        )

        get_stock_prices = PythonOperator(
            task_id=f'get_stock_prices_{symbol}',
            python_callable=_get_stock_prices,
            op_kwargs={'api_url': BaseHook.get_connection('stock_api').host, 'symbol': symbol}
        )

        is_api_available >> get_stock_prices

    trigger_format_stock_prices_dag = TriggerDagRunOperator(
        task_id='trigger_format_stock_prices_dag',
        trigger_dag_id='format_prices',
    )
    
    get_stock_prices >> trigger_format_stock_prices_dag