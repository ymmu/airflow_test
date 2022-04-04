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
topic = 'transactions-data'

'''
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

# Subscribe to 1 topic defaults to the earliest and latest offsets
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{k_conf['host_1']}") \
    .option("subscribe", topic) \
    .load()
df.printSchema()
'''
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
'''
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{k_conf['host_1']}") \
    .option("checkpointLocation", '/data/lucca/git/elt_pipeline/src/pyspark') \
    .option("topic", "streamtest-from-spark") \
    .start() \
    .awaitTermination()