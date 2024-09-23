from airflow.decorators import dag, task
from datetime import datetime

@dag(
	start_date=datetime(2024, 1, 1),
	schedule_interval='@daily',
    catchup=False,
    tags=['taskflow']
)

def taskflow():
	
	@task
	def task_a():
		print("Task A")
		return 40
		
	@task
	def task_b(value):
		print("Task B")
		print(value)

	task_b(task_a())

taskflow()