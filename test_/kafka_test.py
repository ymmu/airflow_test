from kafka import KafkaConsumer

from src import utils_

conf = utils_.get_kafka_config()

consumer = KafkaConsumer('test', bootstrap_servers=f'{conf["host"]}:{conf["port"]}')
for msg in consumer:
    print('sss')
    print (msg)
