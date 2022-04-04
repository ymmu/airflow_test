from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, CheckpointingMode
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime
from src import utils_


k_conf = utils_.get_kafka_config()
z_conf = utils_.get_zookeeper_config()
es_conf = utils_.get_es_config()


def from_kafka_to_kafka_demo():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    s_env.set_parallelism(1)
    s_env.enable_checkpointing(3000, CheckpointingMode.AT_LEAST_ONCE)

    # use blink table planner
    st_env = StreamTableEnvironment \
        .create(s_env, environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .use_blink_planner().build())

    # register source and sink
    register_rides_source(st_env)
    #kafka_source(st_env)  # json deserialize error ...
    #register_rides_sink(st_env)
    es_sink(st_env)

    # query
    st_env.from_path("source").insert_into("sink")

    # execute
    st_env.execute("from_kafka_to_es")


def register_rides_source(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("universal")
            .topic("transactions-data")
            .start_from_earliest()
            .property("zookeeper.connect", f'{z_conf["host_1"]}')
            .property("bootstrap.servers", f'{k_conf["host_1"]}')) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
                DataTypes.FIELD("customer", DataTypes.STRING()),
                DataTypes.FIELD("transaction_type", DataTypes.STRING()),
                DataTypes.FIELD("online_payment_amount", DataTypes.DOUBLE()),
                DataTypes.FIELD("in_store_payment_amount", DataTypes.DOUBLE()),
                DataTypes.FIELD("lat", DataTypes.DOUBLE()),
                DataTypes.FIELD("lon", DataTypes.DOUBLE()),
                DataTypes.FIELD("transaction_datetime", DataTypes.TIMESTAMP(3))]))) \
        .with_schema(  # declare the schema of the table
        Schema()
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


def kafka_source(st_env):
    kafka_ddl = f"""
    CREATE TABLE source (
        customer STRING,
        transaction_type STRING,
        online_payment_amount DOUBLE,
        in_store_payment_amount DOUBLE,
        lat DOUBLE,
        lon DOUBLE,
        transaction_datetime TIMESTAMP
    )WITH (
        'connector' = 'kafka',
        'topic' = 'transactions-data',
        'properties.bootstrap.servers' = '{k_conf["host_1"]}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """

    st_env.execute_sql(kafka_ddl)


def es_sink(st_env):
    es_ddl = f"""
    CREATE TABLE sink (
        customer STRING,
        transaction_type STRING,
        online_payment_amount DOUBLE,
        in_store_payment_amount DOUBLE,
        lat DOUBLE,
        lon DOUBLE,
        transaction_datetime TIMESTAMP
    )WITH (
        'connector' = 'elasticsearch-7',
        'hosts' = 'http://{es_conf["host_1"]}',
        'index' = 'transaction-data',
        'username' = '{es_conf["user"]}',
        'password' = '{es_conf["passwd"]}'
    )
    """
    st_env.execute_sql(es_ddl)


def register_rides_sink(st_env):
    st_env \
        .connect(  # declare the external system to connect to
        Kafka()
            .version("universal")
            .topic("TempResults")
            .property("zookeeper.connect", f"{z_conf['host_1']}")
            .property("bootstrap.servers", f"{k_conf['host_1']}")) \
        .with_format(  # declare a format for this system
        Json()
            .fail_on_missing_field(True)
            .schema(DataTypes.ROW([
            DataTypes.FIELD("rideId", DataTypes.BIGINT()),
            DataTypes.FIELD("taxiId", DataTypes.BIGINT()),
            DataTypes.FIELD("isStart", DataTypes.BOOLEAN()),
            DataTypes.FIELD("lon", DataTypes.FLOAT()),
            DataTypes.FIELD("lat", DataTypes.FLOAT()),
            DataTypes.FIELD("psgCnt", DataTypes.INT()),
            DataTypes.FIELD("rideTime", DataTypes.STRING())
        ]))) \
        .with_schema(  # declare the schema of the table
        Schema()
            .field("rideId", DataTypes.BIGINT())
            .field("taxiId", DataTypes.BIGINT())
            .field("isStart", DataTypes.BOOLEAN())
            .field("lon", DataTypes.FLOAT())
            .field("lat", DataTypes.FLOAT())
            .field("psgCnt", DataTypes.INT())
            .field("rideTime", DataTypes.STRING())) \
        .in_append_mode() \
        .create_temporary_table("sink")


if __name__ == '__main__':
    from_kafka_to_kafka_demo()
