from src.load import get_data
from moto import mock_s3
# from unittest.mock import pandas as pd
from unittest.mock import patch
from unittest.mock import Mock
import pytest
import boto3
import botocore
from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs
import pg8000
# import awswrangler

# Establishing mock AWS credentials
@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


# Tests for get_data subfunction
@mock_s3
def test_creating_mock_s3():
    conn = boto3.client("s3", region_name="us-east-1")
    res = conn.create_bucket(Bucket="test_bucket_29")
    assert res['ResponseMetadata']['HTTPStatusCode'] == 200

@mock_s3
def test_accessing_the_objects_in_the_bucket():
    conn = boto3.client("s3", region_name="us-east-1")
    res = conn.create_bucket(Bucket="test_bucket_29")
    parquet_files = get_data('test_bucket_29')
    assert parquet_files['KeyCount'] == 0

@mock_s3
def test_retreiving_objects_in_the_bucket():
    bucket = "test_bucket_29"
    conn = boto3.client("s3")
    res = conn.create_bucket(Bucket=bucket)
    parquet_files = get_data(bucket)
    # uploading some fake data to bucket
    with open("./load_test_db/hello_test.parquet", "rb") as f:
        conn.upload_fileobj(f, f"{bucket}", "hello.parquet")
    parquet_files = get_data(bucket)
    assert parquet_files['KeyCount'] == 1
    
@mock_s3
def test_retreiving_objects_in_the_bucketss():
    bucket ="rand2"
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)
    with open("./load_test_db/hello_test.parquet", "rb") as f:
        s3_client.upload_fileobj(f, f"{bucket}", "hello.parquet")
    bucket_objects = s3_client.get_object(Bucket =bucket, Key = "hello.parquet")
    print(bucket_objects)

    file_paths = [f's3://{bucket}/{obj["Key"]}' for obj in bucket_objects['Contents']]
    for file_path in file_paths:
        s3_file = pq.ParquetDataset(file_path)
        table = s3_file.read().to_pandas()
        df= pd.DataFrame(table)
    assert df.columns.tolist() == ['hello']


def test_load_to_warehouse_can_push_to_warehouse():
    pass


def test_load_to_warehouse_throws_helpful_error_on_incorrect_format():
    pass


def test_another_fail_case():
    pass

# Tests for AWS Cloudwatch (possible?)
