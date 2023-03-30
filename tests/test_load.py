from src.load import get_data, make_warehouse_connection
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


@pytest.fixture(scope='function')
def premock_s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

# @pytest.fixture
# def mock_bucket_and_parquet_files(premock_s3):
#     premock_s3.create_bucket(
#         Bucket='scrumptious-squad-in-data-testmock',
#         CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
#     index('config/.env.test')
    # The parquet files are generated in the mock bucket.


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
def test_get_data():
    readtable = pq.read_table('./load_test_db/load.parquet')
    expectedfile = readtable.to_pandas()
    bucket_name = "test_bucket_29"
    file_path = "data/parquet/"
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    # Upload test
    s3.Object(bucket_name, f"{file_path}/load.parquet").put(
        Body=open('./load_test_db/load.parquet', 'rb'))
    s3.Object(bucket_name, f"{file_path}/hello.parquet").put(
        Body=open('./load_test_db/hello_test.parquet', 'rb'))
    # Call get_data() function with test data
    dfs = get_data(bucket_name, file_path)
    pd.testing.assert_frame_equal(expectedfile, dfs["df_load"])


@mock_s3
def test_get_data():
    readtable = pq.read_table('./load_test_db/dim_currency.parquet')
    expectedfile = readtable.to_pandas()
    bucket_name = "test_bucket_29"
    file_path = "data/parquet/"
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    # Upload test
    s3.Object(bucket_name, f"{file_path}/dim_currency.parquet").put(
        Body=open('./load_test_db/dim_currency.parquet', 'rb'))
    # Call get_data() function with test data
    dfs = get_data(bucket_name, file_path)
    pd.testing.assert_frame_equal(expectedfile, dfs["df_dim_currency"])


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


def test_connection_to_warehouse():
    connect = make_warehouse_connection("./config/.env.test")
    assert isinstance(connect, pg8000.Connection)


def test_load__can_push_files_to_warehouse():
    connect = make_warehouse_connection("./config/.env.test")


def test_load_to_warehouse_throws_helpful_error_on_incorrect_format():
    pass


def test_another_fail_case():
    pass

# Tests for AWS Cloudwatch (possible?)
