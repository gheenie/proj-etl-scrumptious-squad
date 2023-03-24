import boto3
from time import time

def make_bucket():
    s3 = boto3.client('s3')
    #bucket_name = f"nicebucket{int(time())}"
    #object = s3.create_bucket(Bucket=bucket_name)
    
    
    s3.delete_bucket(Bucket='nicebucket1679649446')
    s3.delete_bucket(Bucket='nicebucket1679649819')
    #return bucket_name

make_bucket()