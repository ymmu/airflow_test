from datetime import datetime, timedelta
import os
import json
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

_current_path: str = Variable.get("airflow_base_path")
with open(f"{_current_path}/include/json/t1_thirdparty/csv_etl/table_infos.json", "r") as file:
    t1_s3_csv_table_list: json = json.load(file)

default_args = {
    'owner': 'data_dev',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=default_args,
    description='테스트',
    catchup=False,
    schedule_interval="0 23 * * *",
    concurrency=5
) as dag:

    @task.bash
    def run_after_loop() -> str:
        return "echo https://airflow.apache.org/"