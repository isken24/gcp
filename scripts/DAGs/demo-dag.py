from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# Define your custom Python function
def print_hello():
    print("Hello, Airflow!")


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('sample_dag',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

# Define the tasks
start_task = DummyOperator(task_id='start_task', dag=dag)

hello_task = PythonOperator(task_id='hello_task',
                            python_callable=print_hello,
                            dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

# Define the task dependencies
start_task >> hello_task >> end_task
