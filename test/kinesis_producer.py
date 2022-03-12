from pprint import pprint
from datetime import datetime
import boto3
from src import utils_
from kinesis.producer import KinesisProducer
from botocore.config import Config

# boto3 설정
utils_.set_boto3_config()
session = boto3.Session(profile_name='kinesis')
my_config = Config(
    signature_version='v4',
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)
client = session.client('kinesis', config=my_config)
stream_name = utils_.get_kinesis_config()['stream']['test']

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
msg = f'hello kinesis  at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
print(msg)

response = client.put_record(
    StreamName=stream_name,
    Data=bytes(msg, 'utf-8'),
    PartitionKey='test',
    # ExplicitHashKey='string',
    # SequenceNumberForOrdering='string'
)
pprint(response)

# producer = KinesisProducer(stream_name='stream-test')
# producer.put('Hello World from Python')

