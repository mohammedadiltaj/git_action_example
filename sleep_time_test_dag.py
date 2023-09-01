from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() + timedelta(days=-2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG instance
dag = DAG(
    'sleep_time_testing',
    default_args=default_args,
    schedule_interval='@daily',  # Trigger the DAG daily
)

# Define a simple Python function
def print_hello1():
    print("Hello, World one-new")
    time.sleep(300)

def print_hello2():
    print("Hello, World two-new")
    time.sleep(300)

def print_hello3():
    print("Hello, World three-new")
    time.sleep(300)


hello_task1 = PythonOperator(
    task_id='hello_task1',
    python_callable=print_hello1,
    dag=dag,
)

hello_task2 = PythonOperator(
    task_id='hello_task2',
    python_callable=print_hello2,
    dag=dag,
)

hello_task3 = PythonOperator(
    task_id='hello_task3',
    python_callable=print_hello3,
    dag=dag,
)

# Set task dependencies
hello_task1>>hello_task2>>hello_task3
