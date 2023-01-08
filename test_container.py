from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
dag = DAG(
    'my_dag_id',
    default_args=default_args,
    schedule_interval=timedelta(hours=1)
)

# Task that runs a Docker container to add two numbers
add_task = DockerOperator(
    task_id='add_task',
    image='python:latest',
    api_version='auto',
    auto_remove=True,
    command='python -c "print(3+5)"',
    dag=dag
)

# Task that multiplies the result by 2
def multiply(**context):
    result = context['task_instance'].xcom_pull(task_ids='add_task')
    return result * 2

multiply_task = PythonOperator(
    task_id='multiply_task',
    python_callable=multiply,
    provide_context=True,
    dag=dag
)

# Set the order of the tasks
add_task >> multiply_task
