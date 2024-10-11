from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import requests
from include.stock_market.task import _get_stock_prices, _store_prices, _get_symbols_from_MiniO

def _is_api_available() -> PokeReturnValue:
    api = BaseHook.get_connection('stock_api')
    url = f"{api.host}{api.extra_dejson['endpoint']}"
    response = requests.get(url, headers=api.extra_dejson['headers'])
    condition = response.json()['finance']['result'] is None
    return PokeReturnValue(is_done=condition, xcom_value=url)

with DAG(
    dag_id='get_stock_prices',
    catchup=False,
    tags=['get_stock_prices']
) as dag:
    
    symbols = _get_symbols_from_MiniO()
    
    is_api_available = PythonOperator(
        task_id='is_api_available',
        python_callable=_is_api_available,
    )
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'api_url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}', 'symbol': {{ 'symbol' }}},
    )
    
    get_stock_prices.expand(symbol=symbols)
    
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices) }}'}
    )
    
    trigger_formmat_stock_prices_dag = TriggerDagRunOperator(
        task_id='trigger_formmat_stock_prices_dag',
        trigger_dag_id='format_prices',
    )
    
    is_api_available() >> get_stock_prices >> store_prices >> trigger_formmat_stock_prices_dag
