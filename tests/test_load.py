from unittest.mock import patch, Mock
from src.load import get_data, make_warehouse_connection, load_to_warehouse, lambda_handler
from moto import mock_s3
import pytest
import boto3
import os
import io
import pandas as pd
import pyarrow.parquet as pq
import pg8000


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@mock_s3
def test_creating_mock_s3():
    conn = boto3.client("s3", region_name="us-east-1")
    res = conn.create_bucket(Bucket="test_bucket_29")
    assert res['ResponseMetadata']['HTTPStatusCode'] == 200

@mock_s3
def test_get_data():
    readtable = pq.read_table('./load_test_db/dim_date.parquet')
    expectedfile = readtable.to_pandas()
    bucket_name = "test_bucket_29"
    file_path = "data/parquet/"
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    # Uploading test files to mock bucket
    s3.Object(bucket_name, f"{file_path}/dim_date.parquet").put(
        Body=open('./load_test_db/dim_date.parquet', 'rb'))
    s3.Object(bucket_name, f"{file_path}/hello.parquet").put(
        Body=open('./load_test_db/hello_test.parquet', 'rb'))
    dfs = get_data(bucket_name, file_path)
    pd.testing.assert_frame_equal(expectedfile, dfs["df_dim_date"])

@mock_s3
def test_get_data_dim_currency():
    readtable = pq.read_table('./load_test_db/dim_currency.parquet')
    expectedfile = readtable.to_pandas()
    bucket_name = "test_bucket_29"
    file_path = "data/parquet/"
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f"{file_path}/dim_currency.parquet").put(
        Body=open('./load_test_db/dim_currency.parquet', 'rb'))
    dfs = get_data(bucket_name, file_path)
    pd.testing.assert_frame_equal(expectedfile, dfs["df_dim_currency"])


def test_connection_to_warehouse():
    connect = make_warehouse_connection("./config/.env.test")
    assert isinstance(connect, pg8000.Connection)


@mock_s3
@patch('src.load.make_warehouse_connection')
def test_load_to_warehouse(mock_make_connection):
    conn = mock_make_connection
    bucket_name = "test_bucket_31"
    file_path = "data/parquet/"
    dotenv_path = './config/.env.test'
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f"{file_path}/dim_currency.parquet").put(
        Body=open('./load_test_db/dim_currency.parquet', 'rb'))
    dfs = get_data(bucket_name, file_path)
    result = load_to_warehouse(conn, dfs)
    assert result == 'Successfully loaded into data warehouse'

def test_load_to_warehouse_inserted_values():
    dotenv_path = './config/.env.test'
    conn = make_warehouse_connection(dotenv_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM dim_currency")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    assert rows[0] == [0, 'GBP', 'Pound']
    assert rows[1] == [1, 'USD', 'Dollar']
    assert columns == ['currency_id', 'currency_code', 'currency_name']

@mock_s3
def test_lambda_handler():
    bucket_name = "test_bucket_31"
    file_path = "data/parquet/"
    dotenv_path = './config/.env.test'
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f"{file_path}/dim_currency.parquet").put(
        Body=open('./load_test_db/dim_currency.parquet', 'rb'))
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    with patch('src.load.make_warehouse_connection', return_value=mock_connection):
        result = lambda_handler({'bucket_name': bucket_name, 'file_path': file_path, 'dotenv_path': dotenv_path}, None)
        assert 'Successfully loaded into data warehouse' in result['body']

@mock_s3
def test_lambda_handler_error():
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    with patch('src.load.make_warehouse_connection', return_value=mock_connection):
        result = lambda_handler({'bucket_name': 'test-bucket', 'file_path': 'test-data.parquet', 'dotenv_path':'./config/.env.test'}, None)
        assert 'Error loading data to the warehouse' in result['body']
        mock_cursor.execute.assert_not_called()
        mock_connection.commit.assert_not_called()
        mock_cursor.close.assert_not_called()
        mock_connection.close.assert_not_called()
        
def test_load_to_warehouse_throws_helpful_error_on_incorrect_format():
    pass
def test_another_fail_case():
    pass