from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.decorators import dag, task
from datetime import datetime

'''
sqloperator로 테이블 작업
- pd로 작업보다 훨씬 편한거 같은데..? 
- raw data -> bigquery table로 넘어가면 operator만 bigquery로 바꿔서 테이블 업데이트 작업을 자동화 할 수 있지 않을지.
'''

dag = DAG(
    dag_id='lb_pipeline_agg_table_task',
    start_date=datetime(2022, 3, 16),
    # schedule_interval='1 * * * *',
    tags=['lb_pipeline_agg_table_task']
)

start_t = DummyOperator(task_id='start',
                      dag=dag)

artist_track_rank_hourly = MySqlOperator(task_id='update_hourly_artist_track_rank',
                                         mysql_conn_id='mariadb_p_conn',
                                         sql='./sql/artist_track_rank.sql',
                                         params={'start_t': '2017-09-01 14:00:00', 'db': 'sample'},
                                         retries=1,
                                         dag=dag)

end_t = DummyOperator(task_id='finished',
                      dag=dag)

start_t >> artist_track_rank_hourly >> end_t
