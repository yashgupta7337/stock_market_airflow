"""
Example Dag to understand how Airflow Decorators like dag and task work
Similar to Airflow Operators but syntax more similar to Python
"""

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime

def _task_a():
    print('Task A')
    return 42

@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule='@daily',
    description='A simple DAG to understand Airflow Operators vs Airflow Decorators',
    tags=['taskflow']
)

def taskflow():

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=_task_a
    )

    @task
    def task_b(value):
        print('Task B')
        print(value)

    task_b(task_a.output)

taskflow()