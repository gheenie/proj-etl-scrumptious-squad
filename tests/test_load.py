from unittest.mock import patch, Mock
from src.load import get_data, make_warehouse_connection, load_to_warehouse, load_lambda_handler, get_bucket_name, pull_secrets
from moto import mock_s3, mock_secretsmanager
import pytest
import boto3
import os
import io
import pandas as pd
import pyarrow.parquet as pq
import pg8000
import logging
import json
from botocore.exceptions import ClientError


logger = logging.getLogger('test')
logger.setLevel(logging.INFO)
logger.propagate = True



@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"



@pytest.fixture(scope='function')
def wrong_bucket_event():
    with open('tests/test_data/incorrect_bucket.json') as i:
        event = json.loads(i.read())
    return event


@mock_s3
def test_creating_mock_s3():
    conn = boto3.client("s3", region_name="us-east-1")
    res = conn.create_bucket(Bucket="test_bucket_29")
    assert res['ResponseMetadata']['HTTPStatusCode'] == 200


@mock_s3
def test_get_data():
    readtable = pq.read_table('./load_test_db/dim_date.parquet')
    expectedfile = readtable.to_pandas()
    bucket_prefix = 'scrumptious-squad'
    bucket_name = 'scrumptious-squad-testing123'
    file_path = "data/parquet/"
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    # Uploading test files to mock bucket
    s3.Object(bucket_name, f"{file_path}/dim_date.parquet").put(
        Body=open('./load_test_db/dim_date.parquet', 'rb'))
    s3.Object(bucket_name, f"{file_path}/hello.parquet").put(
        Body=open('./load_test_db/hello_test.parquet', 'rb'))
    dfs = get_data(bucket_prefix)
    pd.testing.assert_frame_equal(expectedfile, dfs["df_dim_date"])


@mock_s3
def test_get_data_dim_currency():
    readtable = pq.read_table('./load_test_db/dim_currency.parquet')
    expectedfile = readtable.to_pandas()
    bucket_prefix = "test_bucket"
    bucket_name = "test_bucket_29"
    file_path = "data/parquet/"
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f"{file_path}/dim_currency.parquet").put(
        Body=open('./load_test_db/dim_currency.parquet', 'rb'))
    dfs = get_data(bucket_prefix)
    pd.testing.assert_frame_equal(expectedfile, dfs["df_dim_currency"])

@mock_secretsmanager
def test_connection_to_warehouse():
    secret_id = 'cred_DW'
    user_id = 'dinunimmagadda'
    password = 'Dinuece@7'
    host = 'localhost'
    port = 5432
    database = 'test_warehouse'
    schema = 'team_4'
    secret = {
        'user': user_id,
        'password': password,
        'database': database,
        'host':host,
        'port':port,
        'schema':schema
    }
    secret_manager = boto3.client('secretsmanager')
    secret_string = json.dumps(secret)
    secret_manager.create_secret(Name=secret_id, SecretString=secret_string)
    conn = make_warehouse_connection(secret_id)
    assert isinstance(conn, pg8000.Connection)


@mock_s3
@patch('src.load.make_warehouse_connection')
def test_load_to_warehouse(mock_make_connection):
    conn = mock_make_connection
    bucket_prefix = 'test_bucket'
    bucket_name = "test_bucket_31"
    file_path = "data/parquet/"
    # dotenv_path = './config/.env.test'
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f"{file_path}/dim_currency.parquet").put(
        Body=open('./load_test_db/dim_currency.parquet', 'rb'))
    dfs = get_data(bucket_prefix)
    result = load_to_warehouse(conn, dfs)
    assert result['body'] == 'Successfully loaded into data warehouse'


@mock_s3
def test_load_lambda_handler():
    bucket_prefix = 'test_bucket'
    bucket_name = "test_bucket_31"
    file_path = "data/parquet/"
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f"{file_path}/dim_currency.parquet").put(
        Body=open('./load_test_db/dim_currency.parquet', 'rb'))
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    with patch('src.load.make_warehouse_connection', return_value=mock_connection):
        result = load_lambda_handler(
            {'bucket_prefix': bucket_prefix, 'file_path': file_path, 'secret_id': 'cred_DW'}, None)
        assert 'Successfully loaded into data warehouse' in result['body']


# @mock_s3
# def test_load_lambda_handler_with_non_existing_bucket():
#     s3 = boto3.client('s3')
#     bucket_name = 'test-bucket'
#     s3.create_bucket(Bucket=bucket_name)
#     event = {
#         'bucket_prefix': 'test',
#         'file_path': 'data/parquet',
#         'secret_id': 'secret-id'
#     }

#     # Call the function with the event payload
#     with pytest.raises(ClientError) as e:
#         load_lambda_handler(event, None)

#     # Ensure that the correct error message is returned
#     assert 'NoSuchBucket' in str(e.value)
