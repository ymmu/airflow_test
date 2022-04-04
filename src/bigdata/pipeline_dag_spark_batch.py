from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime

'''
sqloperator로 테이블 작업
- pd로 작업보다 훨씬 편한거 같은데..? 
- raw data -> bigquery table로 넘어가면 operator만 bigquery로 바꿔서 테이블 업데이트 작업을 자동화 할 수 있지 않을지.
'''

dag = DAG(
    dag_id='hourly_spark_batch_task',
    start_date=datetime(2022, 3, 16),
    # schedule_interval='1 * * * *',
    tags=['hourly_spark_batch_task']
)

start_t = DummyOperator(task_id='start', dag=dag)

# 수정중
# jinja template 도 사용해보자..
artist_track_rank_hourly = BashOperator(task_id='hourly_spark_spark_batch_task',
                                        template_searchpath="/data/lucca/git/elt_pipeline/src/bigdata"
                                        bash_command="",
                                        params={},
                                        retries=1,
                                        dag=dag)

end_t = DummyOperator(task_id='finished',
                      dag=dag)

start_t >> artist_track_rank_hourly >> end_t
