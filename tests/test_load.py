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
from io import BytesIO


# bucket_name = "test_bucket_29"

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


# import pyarrow as pa
# @mock_s3
# def test_retreiving_objects_from_the_bucket():
#     bucket = "rand2"
#     s3_client = boto3.client("s3")
#     s3_client.create_bucket(Bucket=bucket)
#     print("bucket successfuly created")
#     with open("./load_test_db/hello_test.parquet", "rb") as f:
#         s3_client.upload_fileobj(f, f"{bucket}", "hello.parquet")
#     bucket_objects = s3_client.get_object(Bucket=bucket, Key="hello.parquet")
#     print(bucket_objects['Body'])
#     buffer = BytesIO()
#     s3_resource=boto3.resource("s3")
#     object= s3_resource.Object(bucket, "hello.parquet")
#     object.download_fileobj(buffer)
#     print(pq.read_table("s3://rand2/hello.parquet"))
#     print(s3_client.list_objects_v2(Bucket=bucket))
#     # s3_file = pq.ParquetDataset("s3://rand2/hello.parquet")
#     # print(s3_file)
    

#     # file_paths = [
#     # f's3://{bucket}/{obj["Key"]}' for obj in bucket_objects['Body']]
#     for file_path in file_paths:
#         s3_file = pq.ParquetDataset(file_path)
#         table = s3_file.read().to_pandas()
#         df = pd.DataFrame(table)
#     assert df.columns.tolist() == ['hello']


def test_load_to_warehouse_can_push_to_warehouse():
    pass


def test_load_to_warehouse_throws_helpful_error_on_incorrect_format():
    pass


def test_another_fail_case():
    pass

# Tests for AWS Cloudwatch (possible?)
