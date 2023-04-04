from unittest.mock import patch, Mock, MagicMock
import unittest.mock as mock
from src.load import pull_secrets,get_bucket_name, get_data, make_warehouse_connection, load_data_to_warehouse, load_lambda_handler
from moto import mock_s3, mock_secretsmanager
import pytest
import boto3
import os
import pandas as pd
import pyarrow.parquet as pq
import json
import logging

logger = logging.getLogger('test')
logger.setLevel(logging.INFO)

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope='function')
def s3_client():
    with mock_s3():
        yield boto3.client('s3')

@mock_secretsmanager
def test_pull_secrets():
    logger.info('Running test_retrival_function')
    secret_id = 'missiles'
    secrets = {
        'host': 'test-missiles',
        'port': 5432,
        'user': 'team_4',
        'password': 'asdf123',
        'database': 'test-db',
        'schema': 'team_4'
    }
    secret_manager = boto3.client('secretsmanager')
    secret_string = json.dumps(secrets)
    secret_manager.create_secret(Name=secret_id, SecretString=secret_string)
    result = pull_secrets(secret_id)
    assert result['user'] == 'team_4'

@mock_s3
def test_get_bucket_name(s3_client):
    logger.info('Running test_get_bucket_name')
    s3_client.create_bucket(Bucket='test-bucket-in-01234567')
    s3_client.create_bucket(Bucket='test-bucket-pr-01234567')
    assert get_bucket_name('test-bucket-in') == 'test-bucket-in-01234567'
    assert get_bucket_name('test-bucket-pr') == 'test-bucket-pr-01234567'
    assert get_bucket_name('foo') is None

    if not s3_client.head_bucket(Bucket='test-bucket-in-01234567'):
        logger.error('Bucket creation failed')



@mock_s3
def test_get_data(s3_client):
    readtable = pq.read_table('./load_test_db/dim_date.parquet')
    expectedfile = readtable.to_pandas()
    bucket_name = "test_bucket_29"
    file_path = "data/parquet/"
    s3_client.create_bucket(Bucket=bucket_name)
    with open('./load_test_db/dim_date.parquet', 'rb') as file:
        s3_client.upload_fileobj(file, bucket_name, 'data/parquet/dim_date.parquet')
    dfs = get_data('test_bucket')
    pd.testing.assert_frame_equal(expectedfile, dfs["df_dim_date"])


@mock_s3
def test_get_data_dim_currrency(s3_client):
    readtable = pq.read_table('./load_test_db/dim_currency.parquet')
    expectedfile = readtable.to_pandas()
    bucket_name = "test_bucket_29"
    bucket_prefix = 'test_bucket'
    file_path = "data/parquet/"
    s3_client.create_bucket(Bucket=bucket_name)
    with open('./load_test_db/dim_currency.parquet', 'rb') as file:
        s3_client.upload_fileobj(file, bucket_name, f"{file_path}/dim_currency.parquet")
    dfs = get_data(bucket_prefix)
    pd.testing.assert_frame_equal(expectedfile, dfs["df_dim_currency"])


@patch('src.load.pg8000.connect')
@patch('src.load.pull_secrets', return_value={
        'host': 'test-missiles',
        'port': 5432,
        'user': 'team_4',
        'password': 'asdf123',
        'database': 'test-db',
})
def test_make_warehouse_connection(mock_pull_secrets, mock_pg8000_connect):
    mock_conn = MagicMock()
    mock_pg8000_connect.return_value = mock_conn
    conn = make_warehouse_connection('mock_secret_id')
    assert conn == mock_conn
    mock_pg8000_connect.assert_called_once_with(
        host  = 'test-missiles',
        user = 'team_4',
        password = 'asdf123',
        database= 'test-db'
    )

@mock_s3
@mock.patch('src.load.make_warehouse_connection')
def test_load_data_to_warehouse(mock_make_connection,s3_client):
    bucket_prefix = 'test'
    mock_conn = mock.MagicMock()
    mock_make_connection.return_value = mock_conn
    bucket_name = 'test-bucket'
    s3_client.create_bucket(Bucket=bucket_name)
    with open('./load_test_db/dim_currency.parquet', 'rb') as file:
        s3_client.upload_fileobj(file, bucket_name, 'data/parquet/dim_currency.parquet')
    result = load_data_to_warehouse('mock_secret_id', bucket_prefix)
    assert result == {'statusCode': 200, 'body': 'Successfully loaded into data warehouse'}



@pytest.fixture
def event():
    return {
        'secret_id': 'test_secret',
        'bucket_prefix': 'test_bucket'
    }

@pytest.fixture
def context():
    return Mock()

@mock_secretsmanager
@mock_s3
def test_lambda_handler(event, context):
    client = boto3.client('secretsmanager')
    client.create_secret(
        Name='test_secret',
        SecretString=json.dumps({
            'host': 'test_host',
            'user': 'test_user',
            'password': 'test_pass',
            'database': 'test_db'
        })
    )
    s3 = boto3.resource('s3', region_name='us-east-1')
    bucket = s3.create_bucket(Bucket='test_bucket')
    with open('test_data.parquet', 'wb') as f:
        f.write(b'test_data')
    bucket.upload_file('test_data.parquet', 'data/parquet/test_data.parquet')
    with patch('src.load.get_data', return_value={'test_data': 'test_df'}), \
         patch('src.load.load_data_to_warehouse', return_value=True):
        result = load_lambda_handler(event, context)
        assert result['statusCode'] == 200
        assert result['body'] == 'Data loaded into warehouse successfully'



@mock_secretsmanager
@mock_s3
def test_lambda_handler_invalid_secret_id():
    bucket_name = 'test-bucket'
    secret_name = 'test-secret'
    secret_value = {
        'host': 'test-host',
        'user': 'test-user',
        'password': 'test-password',
        'database': 'test-database'
    }
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)
    with open('./load_test_db/dim_currency.parquet', 'rb') as file:
        s3.upload_fileobj(file, bucket_name, 'data/parquet/dim_currency.parquet')
    secrets_manager = boto3.client('secretsmanager')
    secrets_manager.create_secret(Name=secret_name, SecretString=json.dumps(secret_value))
    event = {
        'secret_id': 'invalid-secret-id',
        'bucket_prefix': bucket_name
    }
    result = load_lambda_handler(event, None)
    assert result['statusCode'] == 400
    assert result['body'] == 'Error: Failed to load data into warehouse'

@mock_secretsmanager
@mock_s3
def test_lambda_handler_invalid_bucket_prefix():
    bucket_name = 'test-bucket'
    secret_name = 'test-secret'
    secret_value = {
        'host': 'test-host',
        'user': 'test-user',
        'password': 'test-password',
        'database': 'test-database'
    }
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)
    with open('./load_test_db/dim_currency.parquet', 'rb') as file:
        s3.upload_fileobj(file, bucket_name, 'data/parquet/dim_currency.parquet')
    secrets_manager = boto3.client('secretsmanager')
    secrets_manager.create_secret(Name=secret_name, SecretString=json.dumps(secret_value))
    event = {
        'secret_id': secret_name,
        'bucket_prefix': 'invalid-bucket-prefix'
    }
    result = load_lambda_handler(event, None)
    assert result['statusCode'] == 400
    assert result['body'] == 'Error: Failed to load data from S3 bucket'







