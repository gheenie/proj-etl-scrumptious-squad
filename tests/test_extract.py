"""
To form your assertion statements, you need to refer to the
seeded data in extraction-test-db/setup-test-db.txt
"""


from src.extract import (
    pull_secrets, 
    make_connection, 
    get_titles, 
    get_bucket_name, 
    check_table_in_bucket, 
    check_each_table, 
    add_updates, 
    index,
    get_parquet
)
from src.set_up.make_secrets import (entry)
import pandas as pd
import pytest
import os
from moto import (mock_secretsmanager, mock_s3)
import boto3


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture(scope='function')
def premock_secretsmanager(aws_credentials):
    with mock_secretsmanager():
        # yield boto3.client('secretsmanager', region_name='us-east-1')
        yield 'unused string, this is just to prevent mock from closing'


def test_pull_secrets_returns_correct_secrets(premock_secretsmanager):
    entry()
    user, password, database, host, port = pull_secrets()

    assert user == 'project_user_4'
    assert password == 'LC7zJxE3BfvY7p'
    assert database == 'totesys'
    assert host == 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    assert port == 5432


def test_make_connection_connects_to_seeded_db_and_get_titles_returns_correct_table_names():
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
def premock_s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture
def mock_bucket(premock_s3):
    premock_s3.create_bucket(
        Bucket='scrumptious-squad-in-data-testmock',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    
    
def test_get_bucket_name_returns_correct_name__name_exists(mock_bucket, premock_s3):
    premock_s3.create_bucket(
        Bucket='scrumptious-squad-pr-data-actually-different-name',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )

    assert get_bucket_name('scrumptious-squad-in-data-') == 'scrumptious-squad-in-data-testmock'
    assert get_bucket_name('scrumptious-squad-pr-data-') == 'scrumptious-squad-pr-data-actually-different-name'


def test_check_table_in_bucket__0_existing_keys(mock_bucket):
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


def test_check_table_in_bucket__some_keys_exist(mock_bucket, premock_s3):
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
    
    premock_s3.put_object(
        Body='any text', 
        Bucket='scrumptious-squad-in-data-testmock',
        Key='design.parquet'
    )
    premock_s3.put_object(
        Body='any text', 
        Bucket='scrumptious-squad-in-data-testmock',
        Key='sales_order.parquet'
    )

    for title in tables:
        if title in [['design'], ['sales_order']]:
            assert check_table_in_bucket(title) == True
        else:
            assert check_table_in_bucket(title) == False


def test_get_table_and_check_each_table__no_files_exist_yet(mock_bucket):
    """
    Test check_each_table being called for the first time. The method would
    return the prepared DataFrames from the seeded database.
    """

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

    conn = make_connection('config/.env.test')        
    dbcur = conn.cursor()
    to_be_added = check_each_table(tables, dbcur)

    address_df = to_be_added[0]['address']
    design_df = to_be_added[4]['design']
    sales_order_df = to_be_added[8]['sales_order']

    assert len(to_be_added) == 11
    # Test one specific cell
    assert address_df.loc[address_df.address_id == 5][['address_line_1']].values[0] == 'al1-e'
    # Test number of columns
    assert address_df.shape[1] == 10
    # Test number of rows
    assert address_df.shape[0] == 5
    assert design_df.loc[design_df.design_id == 6][['file_name']].values[0] == 'file-f.json'
    assert design_df.shape[1] == 6
    assert design_df.shape[0] == 6
    # Will fail on == '1', which works nicely for testing that a DataFrame preserves data type.
    assert sales_order_df.loc[sales_order_df.sales_order_id == 4][['staff_id']].values[0] == 1
    assert sales_order_df.shape[1] == 12
    assert sales_order_df.shape[0] == 6


def test_push_to_cloud_and_add_updates_correctly_uploads_parquets_to_s3__no_files_exist_yet(mock_bucket, premock_s3):
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
    
    prepared_parquet_filenames = [title[0] + '.parquet' for title in tables]
    prepared_parquet_filenames.sort()
    
    conn = make_connection('config/.env.test')        
    dbcur = conn.cursor()
    to_be_added = check_each_table(tables, dbcur)
    add_updates(to_be_added)
    
    response = premock_s3.list_objects_v2(Bucket='scrumptious-squad-in-data-testmock')
    response_file_names = [content['Key'] for content in (response['Contents'])]
    response_file_names.sort()
    
    assert response_file_names == prepared_parquet_filenames


def test_get_parquet_returns_the_correct_dataframe(mock_bucket, premock_s3):
    """
    Assume file will exist, cause in the main code, get_parquet() 
    will only be called after checking that the tables exist in the bucket.
    """

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

    index('config/.env.test')

    address_df = get_parquet(tables[0][0])
    design_df = get_parquet(tables[4][0])
    sales_order_df = get_parquet(tables[8][0])

    # Test one specific cell
    assert address_df.loc[address_df.address_id == 5][['address_line_1']].values[0] == 'al1-e'
    # Test number of columns
    assert address_df.shape[1] == 10
    # Test number of rows
    assert address_df.shape[0] == 5
    assert design_df.loc[design_df.design_id == 6][['file_name']].values[0] == 'file-f.json'
    assert design_df.shape[1] == 6
    assert design_df.shape[0] == 6
    # Will fail on == '1', which works nicely for testing that a DataFrame preserves data type.
    assert sales_order_df.loc[sales_order_df.sales_order_id == 4][['staff_id']].values[0] == 1
    assert sales_order_df.shape[1] == 12
    assert sales_order_df.shape[0] == 6
