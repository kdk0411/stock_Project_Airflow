from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id='format_prices',
    catchup=False,
    tags=['format_prices']
) as dag:
    
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )
    
    trigger_load_stock_prices_dag = TriggerDagRunOperator(
        task_id='trigger_load_stock_prices_dag',
        trigger_dag_id='load_prices',
    )
    
    format_prices >> trigger_load_stock_prices_dag