from kafka import KafkaProducer
from json import dumps
import time
from src import utils_

conf = utils_.get_kafka_config()
producer = KafkaProducer(acks=0,
                         compression_type='gzip',
                         bootstrap_servers=[f'{conf["host"]}:{conf["port"]}'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))
print('ffff')
start = time.time()
for i in range(1):
    data = {'str' : 'result'+str(i)}
    producer.send('test', value=data)
    producer.flush()
    print("elapsed :", time.time() - start)

#출처: https://needjarvis.tistory.com/607 [자비스가 필요해]