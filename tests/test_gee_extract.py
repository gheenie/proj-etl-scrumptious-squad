"""
To form your assertion statements, you need to refer to the
seeded data in extraction-test-db/setup-test-db.txt
"""


from src.make_parquet import (make_connection, get_titles, check_table_in_bucket, index)
import pandas as pd
import pytest
import os
from moto import mock_s3
import boto3


def test_make_connection_and_get_titles_returns_correct_table_names():
    conn = make_connection('config/.env.test')        
    dbcur = conn.cursor()
    tables = get_titles(dbcur)

    expected = (
        ['address'],
        ['counterparty'],
        ['currency'],
        ['department'],
        ['design'],
        ['payment_type'],
        ['payment'],
        ['purchase_order'],
        ['sales_order'],
        ['staff'],
        ['transaction']
    )

    assert tables == expected


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture(scope='function')
def premock_s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture
def mock_bucket(premock_s3):
    premock_s3.create_bucket(
        Bucket='nicebucket1679649834',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )

    # with open('requirements.txt', 'rb') as f:
    #     premock_s3.put_object(
    #         Body=f, Bucket='nicebucket1679649834',
    #         Key='requirements.txt'
    #     )


def test_check_table_in_bucket(mock_bucket):
    tables = (
        ['address'],
        ['counterparty'],
        ['currency'],
        ['department'],
        ['design'],
        ['payment_type'],
        ['payment'],
        ['purchase_order'],
        ['sales_order'],
        ['staff'],
        ['transaction']
    )

    for title in tables:
        assert check_table_in_bucket(title) == False


@pytest.mark.skip
def test_whole_extract_function(mock_bucket):
    EXTRACTION_SEED_FOLDER = 'database_access/data/parquet'
    SALES_ORDER_FILE = 'sales_order.parquet'

    # Extract test database into .parquet files.
    index('config/.env.test')
    # Read one table into a DataFrame.
    sales_order_table = pd.read_parquet(f'{EXTRACTION_SEED_FOLDER}/{SALES_ORDER_FILE}')
    
    assert sales_order_table.loc[sales_order_table.sales_order_id == 5][['staff_id']].values[0] == 2
