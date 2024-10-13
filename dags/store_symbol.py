from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from include.stock_market.task import _get_most_active_stocks, _store_symbols    

default_args = {
    'start_date': datetime(2024, 10, 11),
    'schedule':'@weekly',
    'retries': 1,
}

with DAG(
    dag_id='store_symbol',
    default_args=default_args,
    catchup=False,
    tags=['store_symbol']
) as dag:
    conn = BaseHook.get_connection('symbol_url')
    base_url = conn.host
    endpoint = 'markets/stocks/most-active/'
    full_url = f"{base_url}/{endpoint}"
    
    check_url = HttpSensor(
        task_id='check_url',
        http_conn_id='symbol_url',
        endpoint=endpoint,
        response_check=lambda response: response.status_code == 200,
        timeout=300,
        poke_interval=30,
        retries=3
    )
    
    get_most_active_stocks = PythonOperator(
        task_id='get_most_active_stocks',
        python_callable=_get_most_active_stocks,
        op_kwargs={'url': full_url},
        do_xcom_push=True
    )
    
    store_symbols = PythonOperator(
        task_id='store_symbols',
        python_callable=_store_symbols,
        op_kwargs={'symbols': "{{ ti.xcom_pull(task_ids='get_most_active_stocks') }}"}
    )
    
    trigger_get_stock_prices_dag = TriggerDagRunOperator(
        task_id='trigger_get_stock_prices_dag',
        trigger_dag_id='get_stock_prices',
    )
    
    check_url >> get_most_active_stocks >> store_symbols >> trigger_get_stock_prices_dag
    