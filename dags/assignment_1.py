from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random

 
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description='Assignment - 1: A simple DAG to generate and check random numbers',
    catchup=False,
    tags=['assignment', 'airflow decorators']
)

def assignment_1():
    @task
    def generate_random_number():
        print('Task - generate_random_number')
        number = random.randint(1, 100)
        print(f"Generated random number: {number}")
        return number
        
    @task
    def check_even_odd(number):
        print('Task - check_even_odd')
        result = "even" if number % 2 == 0 else "odd"
        print(f"The number {number} is {result}.")
 
    check_even_odd(generate_random_number())

assignment_1()