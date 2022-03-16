import boto3
from src.af_batch.src import utils_

utils_.set_boto3_config()
session = boto3.Session(profile_name='s3')
client = session.client('s3')

s3_conf = utils_.get_s3_config()
client.download_file(*s3_conf.values())