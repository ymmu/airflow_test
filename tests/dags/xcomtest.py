##############
#DAG Setting
##############
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

dag = DAG(
        dag_id = "xcomtest",
        start_date = datetime(2021,1,31),
        schedule_interval = '@once',
        tags=['subdir_test']
    )



#############
#Python code
#############


import requests
import logging

# csv파일을 str로 저장
def extract(**context):
    url = context["params"]["url"]
    logging.info(url)
    f = requests.get(url)
    return (f.text)

# extract 함수에서 얻어온 text를 xcom_pull로 가져와 처리함
def transform(**context):
    text = context['task_instance'].xcom_pull(task_ids='exec_extract')
    lines = text.split("\n")
    return lines

####################
# Dag Task Setting
####################

exec_extract = PythonOperator(
        task_id = 'exec_extract',
        python_callable = extract,       
        params={'url': 'https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv'},
        provide_context=True,
        dag = dag
        )
        
exec_transform = PythonOperator(
        task_id = 'exec_transform',
        python_callable = transform,
        provide_context=True,
        dag = dag
        )
        
exec_extract >> exec_transform
