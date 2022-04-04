# %spark.pyspark # zeppelin에서 작업을 스크립트로 옮김

from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, os

# import jar packages -----
# spark_version='3.0.2'
# os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages /spark/jars/elasticsearch-spark-30_2.12-7.12.0.jar'

# import my modules -------
print(sys.path)
sys.path.append('/opt/airflow/dags/elt_pipeline/src')

import utils_

k_conf = utils_.get_kafka_config()
es_conf = utils_.get_es_config()

source_topic = 'transactions-data'

''' topic 데이터 예
{
  "customer": "Christine Rogers",
  "transaction_type": "healthcare",
  "online_payment_amount": 22.27,
  "in_store_payment_amount": 0,
  "lat": 44.77016276759855,
  "lon": 20.57665830200811,
  "transaction_datetime": "2022-04-03T13:13:37Z"
}
'''

# Define schema of json
schema = StructType() \
        .add("customer", StringType()) \
        .add("transaction_type", StringType()) \
        .add("online_payment_amount", FloatType()) \
        .add("in_store_payment_amount", IntegerType()) \
        .add("lat", FloatType()) \
        .add("lon", FloatType()) \
        .add("transaction_datetime", StringType())

# kafka topic "transactions-data"의 모든 데이터를 es 인덱스 aprk-test로 보낸다
df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{k_conf['host_1']}") \
        .option("subscribe", source_topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("value")) \
        .select("value.*") \
        .write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", f"{es_conf['host_1']}") \
        .option("es.nodes.wan.only", "true") \
        .option("es.net.http.auth.user", f"{es_conf['user']}") \
        .option("es.net.http.auth.pass", f"{es_conf['passwd']}") \
        .option("es.resource", "spark-test").mode("append").save()
