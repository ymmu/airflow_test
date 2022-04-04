import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka, Elasticsearch
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, lit
from src import utils_

k_conf = utils_.get_kafka_config()
z_conf = utils_.get_zookeeper_config()

def register_transactions_source(st_env):

    st_env.connect(Kafka()
                   .version("universal")
                   .topic("transactions-data")
                   .start_from_latest()
                   .property("zookeeper.connect", f'{z_conf["host"]}:{z_conf["port"]}')
                   .property("bootstrap.servers", f'{k_conf["host"]}:{k_conf["port"]}')) \
        .with_format(Json()
                    .fail_on_missing_field(True) # 없는 field는 기본으로 null로 채움
                    .schema(DataTypes.ROW([  # 토픽 데이터 스키마 정의
                            DataTypes.FIELD("customer", DataTypes.STRING()),
                            DataTypes.FIELD("transaction_type", DataTypes.STRING()),
                            DataTypes.FIELD("online_payment_amount", DataTypes.DOUBLE()),
                            DataTypes.FIELD("in_store_payment_amount", DataTypes.DOUBLE()),
                            DataTypes.FIELD("lat", DataTypes.DOUBLE()),
                            DataTypes.FIELD("lon", DataTypes.DOUBLE()),
                            DataTypes.FIELD("transaction_datetime", DataTypes.TIMESTAMP(3))]))) \
        .with_schema(Schema()
                    .field("customer", DataTypes.STRING())
                    .field("transaction_type", DataTypes.STRING())
                    .field("online_payment_amount", DataTypes.DOUBLE())
                    .field("in_store_payment_amount", DataTypes.DOUBLE())
                    .field("lat", DataTypes.DOUBLE())
                    .field("lon", DataTypes.DOUBLE())
                    .field("rowtime", DataTypes.TIMESTAMP(3))
        .rowtime(Rowtime()
                .timestamps_from_field("transaction_datetime")
                .watermarks_periodic_bounded(60000))) \
        .in_append_mode() \
        .create_temporary_table("source")



def register_transactions_sink_into_csv(st_env):
    # result_file = "/opt/examples/data/output/output_file.csv"
    result_file = "./output_file.csv"
    if os.path.exists(result_file):
        os.remove(result_file)

        # Register Sink
    st_env.execute_sql("""
            CREATE TABLE mySink (
              customer STRING,
              count_transactions BIGINT,
              total_online_payment_amount BIGINT,
              total_in_store_payment_amount BIGINT,
              lat BIGINT,              
              lon BIGINT,              
              last_transaction_time TIMESTAMP
            ) WITH (
              'connector' = 'filesystem',
              'format' = 'csv',
              'path' = '/tmp/result',
              'sink.rolling-policy.rollover-interval' = '10s'
            )
        """)
    
    '''
    st_env.create_temporary_table("sink",
            TableDescriptor.for_connector('filesystem')
            .schema(Schema.new_builder()
                .column("customer", DataTypes.STRING())
                .column("count_transactions", DataTypes.DOUBLE())               
                .column("total_online_payment_amount", DataTypes.DOUBLE())               
                .column("total_in_store_payment_amount", DataTypes.DOUBLE())               
                .column("lat", DataTypes.DOUBLE())               
                .column("lon", DataTypes.DOUBLE())               
                .column("last_transaction_time", DataTypes.TIMESTAMP(3))
                .bould())
            .option('path', result_file)
            .format(FormatDescriptor.for_format('canal-json').build())
            .build())
    '''

def transactions_job():
    # 환경 먼저
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)

    '''
    set_stream_time_characteristic:
        Sets the time characteristic for all streams create from this environment, 
        e.g., processing time, event time, or ingestion time.
        If you set the characteristic to IngestionTime of EventTime 
        this will set a default watermark update interval of 200 ms. 
        If this is not applicable for your application 
        you should change it using pyflink.common.ExecutionConfig.set_auto_watermark_interval.
    '''
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    st_env = StreamTableEnvironment \
        .create(s_env, environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .build())

    register_transactions_source(st_env)
    register_transactions_sink_into_csv(st_env)

    '''
    st_env.from_path("source") \
        .window(Tumble.over("10.hours").on("rowtime").alias("w")) \
        .group_by("customer, w") \
        .select(col('customer')) \
        .execute_insert("sink_into_csv")

    st_env.execute_sql("app")
    '''

if __name__ == '__main__':
    transactions_job()
