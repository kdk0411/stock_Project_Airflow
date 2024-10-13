from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from include.stock_market.task import _get_symbols_from_MiniO, _format_prices, BUCKET_NAME
import re
with DAG(
    dag_id='format_prices',
    catchup=False,
    max_active_tasks=1,
    tags=['format_prices']
) as dag:
    
    symbols = _get_symbols_from_MiniO()
    format_price_tasks = []
    
    for symbol in symbols:
        re_symbol = re.sub(r'[^a-zA-Z0-9_.-]', '_', symbol)
        format_prices = PythonOperator(
            task_id=f'format_prices_{symbol}',
            python_callable=_format_prices,
            op_kwargs={'symbol': symbol}
        )

        format_price_tasks.append(format_prices)
    
    all_format_prices_completed = DummyOperator(
        task_id='all_format_prices_completed'
    )

    for task in format_price_tasks:
        task >> all_format_prices_completed
    
    trigger_load_stock_prices_dag = TriggerDagRunOperator(
        task_id='trigger_load_stock_prices_dag',
        trigger_dag_id='load_prices',
    )
    
    all_format_prices_completed >> trigger_load_stock_prices_dag