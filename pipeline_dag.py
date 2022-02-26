from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime
import requests, sys
import logging


dag = DAG(
    dag_id="xcomtest_2",
    start_date=datetime(2021, 2, 25),
    #schedule_interval='10 * * * *',
    tags=['pipeline_user_data_test']
)

# import my modules
print(sys.path)
sys.path.append('/opt/airflow/dags/elt_pipeline/src')

@task
def get_music_data(**context):
    from multiprocessing import Pool, cpu_count
    import numpy as np
    import pandas as pd
    import spotify_

    # df = context['task_instance'].xcom_pull(task_ids='get_rawdata')
    # batch = df[['artist_name', 'track_name']].to_dict(orient='split')['data']
    # print(batch)
    # # global num_cores
    # num_cores = cpu_count()
    # with Pool(num_cores) as p:
    #     rst = p.starmap_async(spotify_.get_track_data, batch)
    #     print('*'*10)
    #     pprint(rst.get())


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


def transform():
    
    trans_={}
    def preprocess(**context):
        # extract 함수에서 얻어온 data를 xcom_pull로 가져와 처리함
        table = context["params"]["table"]
        df = context['task_instance'].xcom_pull(task_ids='get_rawdata')
        import user, track, artist

        if table == 'user': return user.preprocess(df)
        elif table == 'artist': return artist.preprocess(df)
        elif table == 'track': return track.preprocess(df)
        else: return None

    for i in ['user','artist','track']:
        
        trans_[i] = PythonOperator(
            task_id=f'{i}_preprocess',
            python_callable=preprocess,
            params={'table': i},
            provide_context=True,
            trigger_rule='all_success',
            dag=dag
        )
    
    return trans_



def store():
    
    store_ = {}
    def store_data(**context):
        table = context["params"]["table"]
        df = context['task_instance'].xcom_pull(task_ids=f'{table}_preprocess')
        import user, track, artist, user_history

        if table == 'user': return user.store(df)
        elif table == 'artist': return artist.store(df)
        elif table == 'track': return track.store(df)
        elif table == 'user_history':
            df = context['task_instance'].xcom_pull(task_ids='user_store')
            return user_history.store(df)
        else: return None
    
    for i in ['user','artist','track','user_history']:
        store_[i] = PythonOperator(
            task_id=f'{i}_store',
            python_callable=store_data,
            params={'table': i},
            provide_context=True,
            trigger_rule='all_success',
            dag=dag
        )

    return store_

trans_ = transform()
store_ = store()

exec_extract >> trans_['user'] >> store_['user'] >> store_['user_history']
exec_extract >> trans_['artist'] >> store_['artist'] >> store_['track'] >> store_['user_history']
exec_extract >> trans_['track'] >> store_['track'] 
