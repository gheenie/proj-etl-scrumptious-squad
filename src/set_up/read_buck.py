import boto3
from time import time
import pandas as pd
from io import BytesIO


s3 = boto3.client('s3')
response = s3.list_buckets()
buffer = BytesIO()
bucketname = [bucket['Name'] for bucket in response['Buckets']][0]


files =s3.list_objects_v2(Bucket=bucketname)
objects = [file['key'] for file in files['contents']]

for filename in files:
    buffer = BytesIO()
    client = boto3.resource('s3')
    

data = s3.get_object(Bucket=bucketname, Key=file['Key'])
contents = data['Body'].read()
print(contents)
print(contents.decode("buffer"))





