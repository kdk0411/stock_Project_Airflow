from airflow import DAG
from airflow.operators.python import PythonOperator

from include.stock_market.task import BUCKET_NAME, _get_formatted_csv
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

with DAG(
    dag_id='load_prices',
    catchup=False,
    tags=['load_prices']
) as dag:
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={'path': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'}
    )
    
    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(path=f"s3://{BUCKET_NAME}/{{{{ task_instance.xcom_pull(task_ids='get_formatted_csv') }}}}", conn_id='minio'),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        )
    )
    
    get_formatted_csv >> load_to_dw