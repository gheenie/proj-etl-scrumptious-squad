"""
Most of the tests on the seeded data are currently for very happy
paths - only one or two tables are tested.

To form further assertion statements, refer to the seeded data
in extraction-test-db/setup-test-db.txt
"""

import os
from unittest.mock import patch

from moto import (mock_secretsmanager, mock_s3)
import pytest
import boto3
import pandas as pd

from src.extract import (
    pull_secrets,
    make_connection,
    get_titles,
    get_bucket_name,
    check_table_in_bucket,
    check_each_table,
    add_updates,
    extract_lambda_handler,
    get_parquet,
    get_most_recent_time,
    get_file_info_in_bucket
)
from src.set_up.make_secrets import (
    entry_test_db
)


@pytest.fixture(scope='function')
def aws_credentials():
    """Mocks AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture(scope='function')
def premock_secretsmanager(aws_credentials):
    """Patches any boto3 secretsmanager in this and imported modules."""

    with mock_secretsmanager():
        # yield boto3.client('secretsmanager', region_name='us-east-1')
        yield 'unused string, this is just to prevent mock from closing'


def test_pull_secrets_returns_correct_secrets(premock_secretsmanager):
    """
    Sets up secrets in the patched SecretsManager,
    calls pull_secrets() to retrieve them
    and checks the credentials it returns.
    """

    # Uploads hardcoded test values to SecretsManager
    entry_test_db()

    # Retrieve secrets using the hardcoded identifier
    database, user, password, host, port = pull_secrets('github_actions_DB')

    assert user == 'github_actions_user'
    assert password == 'github_actions_password'
    assert database == 'github_actions_database'
    assert host == 'localhost'
    assert port == '5432'


def test_make_connection_and_get_titles_returns_correct_table_names():
    """
    Connects to the test totesys database, calls get_titles()
    to retrieve the seeded table names
    and checks what it returned.

    Note that load_env() will be called within make_connection, which sets
    env variables for a PROCESS. This means any future tests in this module
    will use these values even after calling load_env() again,
    unless the variables are explicitly popped.
    """

    # Connect to the local test totesys database
    conn = make_connection('config/.env.test')
    dbcur = conn.cursor()
    # Retrieve table names from the connected database
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
    """Patches any boto3 s3 in this and imported modules."""

    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture
def mock_bucket(premock_s3):
    """Uses the patched boto3 s3 to create a mock bucket."""

    premock_s3.create_bucket(
        Bucket='scrumptious-squad-in-data-testmock',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )


def test_get_bucket_name_returns_correct_name(mock_bucket, premock_s3):
    """
    Calls get_bucket_name with bucket name prefixes to retrieve the bucket's
    full name and checks what it returned.
    """

    premock_s3.create_bucket(
        Bucket='scrumptious-squad-pr-data-actually-different-name',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
    )
    expected = 'scrumptious-squad-pr-data-actually-different-name'
    assert get_bucket_name(
        'scrumptious-squad-in-data-') == 'scrumptious-squad-in-data-testmock'
    assert get_bucket_name(
        'scrumptious-squad-pr-data-') == expected


def test_get_file_info_in_bucket_and_check_table_in_bucket(mock_bucket):
    """
    get_file_info_in_bucket gets file names from a mocked bucket
    containing no files. check_table_in_bucket checks that a table name
    is in the file names, which should return false.
    """

    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
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

    # Get the response JSON from listing an S3 bucket's contents
    response = get_file_info_in_bucket(bucketname)

    for title in tables:
        # Check if the table name is among the response JSON's files
        assert check_table_in_bucket(title, response) is False


def test_get_file_info_in_bucket_and_check_keys_exist(mock_bucket, premock_s3):
    """
    Now the mocked bucket contains files, so table names should return True.
    """

    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
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
    # Get the response JSON from listing an S3 bucket's contents
    response = get_file_info_in_bucket(bucketname)

    for title in tables:
        # Check if the table name is among the response JSON's files
        if title in [['design'], ['sales_order']]:
            assert check_table_in_bucket(title, response) is True
        else:
            assert check_table_in_bucket(title, response) is False


def test_get_table_and_check_each_table__no_files_exist_yet(mock_bucket):
    """
    Calls get_table and check_each_table for the 'first time' i.e. all
    seeded data is captured, not just the most recent.
    They return the tables from the seeded Totesys database as DataFrames.
    Check a few of those DataFrames.
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
    # Connect to the local test totesys database
    conn = make_connection('config/.env.test')
    dbcur = conn.cursor()
    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')

    # Get the rows to be added to .parquet files for each table as df in a dict
    to_be_added = check_each_table(tables, dbcur, bucketname)
    address_df = to_be_added[0]['address']
    design_df = to_be_added[4]['design']
    sales_order_df = to_be_added[8]['sales_order']

    assert len(to_be_added) == 11

    # Test number of columns
    assert address_df.shape[1] == 10
    # Test number of rows
    assert address_df.shape[0] == 5
    # Test one specific cell
    assert address_df.loc[address_df.address_id ==
                          5][['address_line_1']].values[0] == 'al1-e'

    assert design_df.shape[1] == 6
    assert design_df.shape[0] == 6
    assert design_df.loc[design_df.design_id == 6][[
        'file_name']].values[0] == 'file-f.json'

    # Will fail on == '1', which works for testing that a df preserves type
    assert sales_order_df.loc[sales_order_df.sales_order_id == 4][[
        'staff_id']].values[0] == 1
    assert sales_order_df.shape[1] == 12
    assert sales_order_df.shape[0] == 6


def test_push_to_cloud_and_add_updates_uploads_to_s3(mock_bucket, premock_s3):
    """
    Calls push_to_cloud and add_updates for the 'first time' i.e.
    the mock bucket does not have existing files that each represents
    a table from the seeded Totesys database.

    This test isn't inspecting the contents of the uploaded files,
    only that the files are present in the bucket.
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
    # Connect to the local test totesys database
    conn = make_connection('config/.env.test')
    dbcur = conn.cursor()
    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    # Get the rows to be added to .parquet files for each table as df in a dict
    to_be_added = check_each_table(tables, dbcur, bucketname)

    # .parquet file names are just the table name and the file extension
    prepared_parquet_filenames = [title[0] + '.parquet' for title in tables]
    # Sort to make assertion easier
    prepared_parquet_filenames.sort()

    # Convert the rows to be added for each table from DataFrames to parquets
    # and upload to an S3 bucket
    add_updates(to_be_added, bucketname)
    # Retrieve the names of the files that were just added
    response = premock_s3.list_objects_v2(
        Bucket='scrumptious-squad-in-data-testmock')
    response_file_names = [content['Key']
                           for content in (response['Contents'])]
    # Sort to make assertion easier
    response_file_names.sort()

    assert response_file_names == prepared_parquet_filenames


def test_get_parquet_returns_the_correct_dataframe(mock_bucket, premock_s3):
    """
    Retrieves the file contents from the mock bucket which contains
    files that each represents a table from the seeded Totesys database.

    Assume that the files will exist, cause in the main code, get_parquet()
    will only be called after checking that the tables exist in the bucket.
    """

    # Execute extraction once with the seeded Totesys database
    extract_lambda_handler({'dotenv_path_string': 'config/.env.test'})
    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    # Get the response JSON from listing an S3 bucket's contents
    response = get_file_info_in_bucket(bucketname)

    # Get the data from .parquets in an S3 bucket as DataFrames
    address_df = get_parquet('address', bucketname, response)
    design_df = get_parquet('design', bucketname, response)
    sales_order_df = get_parquet('sales_order', bucketname, response)

    # Test number of columns
    assert address_df.shape[1] == 10
    # Test number of rows
    assert address_df.shape[0] == 5
    # Test one specific cell
    assert address_df.loc[address_df.address_id ==
                          5][['address_line_1']].values[0] == 'al1-e'

    assert design_df.shape[1] == 6
    assert design_df.shape[0] == 6
    assert design_df.loc[design_df.design_id == 6][[
        'file_name']].values[0] == 'file-f.json'

    # Will fail on == '1', which works for testing that a df preserves type
    assert sales_order_df.loc[sales_order_df.sales_order_id == 4][[
        'staff_id']].values[0] == 1
    assert sales_order_df.shape[1] == 12
    assert sales_order_df.shape[0] == 6


def test_when_most_recent_entry_is_last_row(mock_bucket):
    """
    Test for function get_most_recent_time
    Extracts from the seed Totesys database once. Then calls
    get_most_recent_time to retrieve the latest 'created_at'
    and 'last_updated' times.

    Assert on one table only. The latest times happen to be in the last row.
    """

    # Execute extraction once with the seeded Totesys database
    extract_lambda_handler({'dotenv_path_string': 'config/.env.test'})
    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    # Get the response JSON from listing an S3 bucket's contents
    response = get_file_info_in_bucket(bucketname)

    # Get the latest timestamps for the 'created_at' and 'last_updated' columns
    # from a .parquet file that represents a table
    most_recent_times_sales_order = get_most_recent_time(
        ['sales_order'], bucketname, response)

    assert most_recent_times_sales_order == {'created_at': pd.Timestamp(
        2023, 1, 1, 10), 'last_updated': pd.Timestamp(2023, 1, 1, 10)}


@patch('src.extract.make_connection')
def test_when_most_recent_entry_isnt_last_row(mock_connection, mock_bucket):
    """
    Test for function get_most_recent_time

    Now the latest times are not are not in the last row.

    This test requires patching because of the way import works -
    when src.extract is imported to this test file, it is run once.
    Hence the seed Totesys database in its default
    state is used to interpret conn and dbcur for
    the rest of the logic in src.extract.
    The first lines of code in this test changes the seed Totesys
    database's state,  but this is only happening after that first run.
    To make src.extract use the new state, it needs to be patched,unless the
    dbcur created in this test can be passed to the calls during ACT phase.
    """

    # Insert new entries into the seed Totesys database
    conn = make_connection('config/.env.test')
    dbcur = conn.cursor()
    query_string = '''INSERT INTO sales_order
                      (sales_order_id, created_at, last_updated,design_id,
                        staff_id, counterparty_id, units_sold, unit_price,
                        currency_id, agreed_delivery_date, agreed_payment_date,
                        agreed_delivery_location_id)
                      VALUES
                      (7, '2023-02-02 11:30:00.000000',
                      '2023-01-01 10:00:00.000000', 1, 4, 3, 50, 6.00, 2,
                      '2023-09-09', '2023-09-09', 5),
                      (8, '2023-01-01 10:00:00.000000',
                      '2023-03-03 08:45:00.000000', 7, 3, 2, 40, 5.00, 3,
                      '2023-09-09', '2023-09-09', 1),
                      (9, '2023-01-01 10:00:00.000000',
                      '2023-01-01 10:00:00.000000', 4, 2, 1, 30, 4.00, 1,
                      '2023-09-09', '2023-09-09', 3);
                   '''
    dbcur.execute(query_string)

    # Patch connections and imported modules to use the updated seed Totesys db
    mock_connection.return_value = conn
    # Execute extraction once with the updated seed Totesys database
    extract_lambda_handler({'dotenv_path_string': 'config/.env.test'})
    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    # Get the response JSON from listing an S3 bucket's contents
    response = get_file_info_in_bucket(bucketname)

    # Get the latest timestamps for the 'created_at' and 'last_updated' columns
    # from a .parquet file that represents a table
    most_recent_times_sales_order = get_most_recent_time(
        ['sales_order'], bucketname, response)

    assert most_recent_times_sales_order == {'created_at': pd.Timestamp(
        2023, 2, 2, 11, 30), 'last_updated': pd.Timestamp(2023, 3, 3, 8, 45)}


def test_get_table_and_check_each_table_when_no_incoming_files(mock_bucket):
    """
    See test_get_table_and_check_each_table__no_files_exist_yet.

    Now both methods are called after one extraction.

    Only retrieves rows that are new compared to the first extraction as df.

    Currently only one table is updated in this test.
    """

    # Execute extraction once with the seeded Totesys database
    extract_lambda_handler({'dotenv_path_string': 'config/.env.test'})

    # Insert new entries into the seed Totesys database
    conn = make_connection('config/.env.test')
    dbcur = conn.cursor()
    query_string = '''INSERT INTO sales_order
                      (sales_order_id, created_at, last_updated, design_id,
                        staff_id, counterparty_id, units_sold, unit_price,
                        currency_id, agreed_delivery_date, agreed_payment_date,
                        agreed_delivery_location_id)
                      VALUES
                      (7, '2023-02-02 11:30:00.000000',
                      '2023-01-01 10:00:00.000000', 1, 4, 3, 50, 6.00, 2,
                      '2023-09-09', '2023-09-09', 5),
                      (8, '2023-01-01 10:00:00.000000',
                      '2023-03-03 08:45:00.000000', 7, 3, 2, 40, 5.00, 3,
                      '2023-09-09', '2023-09-09', 1),
                      (9, '2023-01-01 10:00:00.000000',
                      '2023-01-01 10:00:00.000000', 4, 2, 1, 30, 4.00, 1,
                      '2023-09-09', '2023-09-09', 3);
                   '''
    dbcur.execute(query_string)

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
    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')

    # Get the rows to be added to .parquet files for each table as df in dicts.
    # dbcur is pointing to the updated seed database
    to_be_added = check_each_table(tables, dbcur, bucketname)

    # Only 1 table is updated here,the index doesn't follow the tables var
    sales_order_df = to_be_added[0]['sales_order']

    assert len(to_be_added) == 1

    # Test number of columns
    assert sales_order_df.shape[1] == 12
    # Test number of rows.
    # 3 rows were added earlier, but 1 doesn't have created_at or last_updated
    assert sales_order_df.shape[0] == 2
    # Test specific cells
    assert sales_order_df.loc[sales_order_df.sales_order_id == 7][[
        'currency_id']].values[0] == 2
    assert sales_order_df.loc[sales_order_df.sales_order_id == 7][[
        'agreed_delivery_date']].values[0] == '2023-09-09'
    assert sales_order_df.loc[sales_order_df.sales_order_id == 8][[
        'last_updated']].values[0] == pd.Timestamp(2023, 3, 3, 8, 45)
    assert sales_order_df.loc[sales_order_df.sales_order_id == 8][[
        'design_id']].values[0] == 7
    assert sales_order_df.loc[sales_order_df.sales_order_id == 8][[
        'unit_price']].values[0] == 5.00


def test_push_to_cloud_add_updates_with_some_files(mock_bucket, premock_s3):
    """
    See test_push_to_cloud_and_add_updates_uploads_to_s3().

    Now both methods are called after one extraction.

    This test does inspect the content of the uploaded files.

    Currently only one table is updated in this test.
    """

    # Execute extraction once with the default seed Totesys database
    extract_lambda_handler({'dotenv_path_string': 'config/.env.test'})

    # Insert new entries into the seed Totesys database
    conn = make_connection('config/.env.test')
    dbcur = conn.cursor()
    query_string = '''INSERT INTO sales_order
                      (sales_order_id, created_at, last_updated, design_id,
                        staff_id, counterparty_id, units_sold, unit_price,
                        currency_id, agreed_delivery_date, agreed_payment_date,
                        agreed_delivery_location_id)
                      VALUES
                      (7, '2023-02-02 11:30:00.000000',
                      '2023-01-01 10:00:00.000000', 1, 4, 3, 50, 6.00, 2,
                      '2023-09-09', '2023-09-09', 5),
                      (8, '2023-01-01 10:00:00.000000',
                      '2023-03-03 08:45:00.000000', 7, 3, 2, 40, 5.00, 3,
                      '2023-09-09', '2023-09-09', 1),
                      (9, '2023-01-01 10:00:00.000000',
                      '2023-01-01 10:00:00.000000', 4, 2, 1, 30, 4.00, 1,
                      '2023-09-09', '2023-09-09', 3);
                   '''
    dbcur.execute(query_string)

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
    # Get full bucket name from a prefix
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    # Get the rows to be added to .parquet files for table df in a dict.
    # dbcur is pointing to the updated seed database
    to_be_added = check_each_table(tables, dbcur, bucketname)

    # Convert the rows to be added for each table from DataFrames to parquets
    # and upload to an S3 bucket
    add_updates(to_be_added, bucketname)

    # Get the response JSON from listing an S3 bucket's contents
    response = get_file_info_in_bucket(bucketname)
    # Get the data from .parquets in an S3 bucket as DataFrames
    sales_order_df = get_parquet('sales_order', bucketname, response)

    # Test number of columns
    assert sales_order_df.shape[1] == 12
    # Test number of rows
    print(sales_order_df)
    assert sales_order_df.shape[0] == 2
    # Test specific cells
    assert sales_order_df.loc[sales_order_df.sales_order_id == 7][[
        'currency_id']].values[0] == 2
    assert sales_order_df.loc[sales_order_df.sales_order_id == 7][[
        'agreed_delivery_date']].values[0] == '2023-09-09'
    assert sales_order_df.loc[sales_order_df.sales_order_id == 8][[
        'last_updated']].values[0] == pd.Timestamp(2023, 3, 3, 8, 45)
    assert sales_order_df.loc[sales_order_df.sales_order_id == 8][[
        'design_id']].values[0] == 7
    assert sales_order_df.loc[sales_order_df.sales_order_id == 8][[
        'unit_price']].values[0] == 5.00
