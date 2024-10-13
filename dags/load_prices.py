from airflow import DAG
from airflow.operators.python import PythonOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

from include.stock_market.task import BUCKET_NAME, _get_formatted_csv, _get_symbols_from_MiniO

with DAG(
    dag_id='load_prices',
    catchup=False,
    max_active_tasks=1,
    tags=['load_prices']
) as dag:
    symbols = _get_symbols_from_MiniO()
    
    for symbol in symbols:
        get_formatted_csv = PythonOperator(
                task_id=f'get_formatted_csv{symbol}',
                python_callable=_get_formatted_csv,
                op_kwargs={'path': f'{BUCKET_NAME}/{symbol}'}
            )
        
        load_to_dw = aql.load_file(
            task_id=f'load_to_dw{symbol}',
            input_file=File(path=f"s3://{BUCKET_NAME}/{symbol}/formatted_prices.csv", conn_id='minio'),
            output_table=Table(
                name=f'stock_market_{symbol.lower()}',
                conn_id='postgres',
                metadata=Metadata(
                    schema='public'
                )
            )
        )
        
        get_formatted_csv >> load_to_dw
        