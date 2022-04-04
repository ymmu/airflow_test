from time import sleep
from pprint import pprint
import boto3
from src.af_batch.src import utils_

utils_.set_boto3_config()
session = boto3.Session(profile_name='kinesis')
client = session.client('kinesis')

stream_name = utils_.get_kinesis_config()['stream']['test']
response = client.describe_stream(StreamName=stream_name)
pprint(response)

# for shard in response['StreamDescription']['Shards']:
# 테스트시 해싱처리로 인해 2번 shard에만 데이터가 들어가 지정해줌.
my_shard_id = response['StreamDescription']['Shards'][2]['ShardId']

pprint(f'my_shard_id: {my_shard_id}')
shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                           ShardId=my_shard_id,
                                           ShardIteratorType='TRIM_HORIZON')#,
                                           #Timestamp=datetime(2022, 2, 27))

# print(shard_iterator)

my_shard_iterator = shard_iterator['ShardIterator']
record_response = client.get_records(ShardIterator=my_shard_iterator, Limit=10)
print('record_response:')
pprint(record_response)

records = record_response['Records']
if len(records) > 0:
    for x in records:
        print('data: {}'.format(x['Data']))
    print('===============')

# 데이터 받아오는데 시간이 걸릴 수 있음.
while 'NextShardIterator' in record_response:
    record_response = client.get_records(ShardIterator=record_response['NextShardIterator'],
                                         Limit=10)

    records = record_response['Records']
    if len(records) > 0:
        for x in records:
            print('data: {}'.format(x['Data']))
        print('===============')

    # wait for 5 seconds
    sleep(1)

