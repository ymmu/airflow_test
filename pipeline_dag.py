from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
from src import utils_
from src import user

dag = DAG(
    dag_id="xcomtest",
    start_date=datetime(2021, 2, 18),
    schedule_interval='@minute',
    tags=['pipeline_user_data_test']
)


def extract():
    return utils_.get_data()


exec_extract = PythonOperator(
    task_id='get_rawdata',
    python_callable=utils_.get_data,
    # params={'url': 'https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv'},
    provide_context=True,
    dag=dag
)


# extract 함수에서 얻어온 data를 xcom_pull로 가져와 처리함
def preprocess(**context):
    df = context['task_instance'].xcom_pull(task_ids='get_rawdata')
    return user.preprocess(df)


preprocess_ = PythonOperator(
    task_id='preprocess',
    python_callable=preprocess,
    provide_context=True,
    dag=dag
)


def store_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='preprocess')
    return user.store_user(df)


store_data_ = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag
)

exec_extract >> preprocess_ >> store_data_
