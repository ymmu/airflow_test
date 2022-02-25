from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests, sys
import logging
#from elt_pipeline.src import utils_
#from elt_pipeline.src import user
#import utils_

dag = DAG(
    dag_id="xcomtest_2",
    start_date=datetime(2021, 2, 25),
    #schedule_interval='10 * * * *',
    tags=['pipeline_user_data_test']
)

print(sys.path)
sys.path.append('/opt/airflow/dags/elt_pipeline/src')

#cmd_ = '''
#    export PYTHONPATH=/opt/airflow/dags/elt_pipeline/env/bin:/opt/airflow/dags/elt_pipeline/env/lib/python3.7/site-packages'''
#t1 = BashOperator(
#    task_id='add_env',
#    bash_command=cmd_
#    )


def extract():
    import utils_
    return utils_.get_data()


exec_extract = PythonOperator(
    task_id='get_rawdata',
    python_callable=extract, #utils_.get_data,
    # params={'url': 'https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv'},
    provide_context=True,
    dag=dag
)


# extract 함수에서 얻어온 data를 xcom_pull로 가져와 처리함
def preprocess(**context):
    import user
    df = context['task_instance'].xcom_pull(task_ids='get_rawdata')
    return user.preprocess(df)


preprocess_ = PythonOperator(
    task_id='preprocess',
    python_callable=preprocess,
    provide_context=True,
    dag=dag
)


def store_data(**context):
    import user
    df = context['task_instance'].xcom_pull(task_ids='preprocess')
    return user.store_user(df)


store_data_ = PythonOperator(
    task_id='store_data',
    python_callable=store_data,
    provide_context=True,
    dag=dag
)

exec_extract >> preprocess_ >> store_data_
