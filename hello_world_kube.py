from airflow import DAG
from airflow.operators.kubernetes_operator import KubernetesOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hello_world_kubernetes',
    default_args=default_args,
    schedule_interval=timedelta(minutes=60)
)

task = KubernetesOperator(
    namespace='default',
    image="python:3.7",
    cmds=["python", "-c"],
    arguments=["print('Hello, World!')"],
    labels={"foo": "bar"},
    name="hello-world",
    task_id="hello-world",
    get_logs=True,
    dag=dag
)
