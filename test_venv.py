from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Use of the DockerOperator',
    'depend_on_past'        : False,
    'start_date'            : datetime(2021, 5, 1),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=5)
}

with DAG('lucca_docker_operator_dag',
         default_args=default_args,
         schedule_interval="5 * * * *",
         catchup=False) as dag:

    start_dag = DummyOperator(
        task_id='start_dag'
        )

    t1 = BashOperator(
        task_id='print_current_date',
        bash_command='./test_venv.py'
    )

    start_dag >> t1