from src.transform import *
import pandas as pd
from src.extract import (index, get_parquet)
import pytest
import datatest as dt
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
def premock_s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture
def mock_bucket_and_parquet_files(premock_s3):
    premock_s3.create_bucket(
        Bucket='scrumptious-squad-in-data-testmock',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
    index('config/.env.test')
    # The parquet files are generated in the mock bucket.


def test_dim_date():
    start_date = '2023-03-26'
    end_date = '2023-03-27'
    expected_output = pd.DataFrame({
        'date_id': pd.to_datetime(['2023-03-26', '2023-03-27']),
        'year': [2023, 2023],
        'month': [3, 3],
        'day': [26, 27],
        'day_of_week': [7, 1],
        'day_name': ['Sunday', 'Monday'],
        'month_name': ['March', 'March'],
        'quarter': [1, 1]
    })
    dim_date = create_dim_date(start_date, end_date)
    pd.testing.assert_frame_equal(dim_date,expected_output)

def test_dim_location(mock_bucket_and_parquet_files):
    df_address = get_parquet('address')
    dim_location = create_dim_location(df_address)
    assert dim_location.shape[1] == 8
    assert dim_location['address_line_1'][3] == 'al1-d'
    assert dim_location['postal_code'][1] == '22222-2222' 
    assert dim_location['city'][0] == 'city-a'
    assert dim_location['phone'][4] == '0000 000005'

def test_dim_design(mock_bucket_and_parquet_files):
    df_design = get_parquet('design')
    dim_design = create_dim_design(df_design)
    assert dim_design.shape[1] == 4
    assert dim_design['design_name'][2] == 'design-c'
    assert dim_design['file_location'][0] == '/aa'
    assert dim_design['file_name'][5] == 'file-f.json'

def test_dim_currency(mock_bucket_and_parquet_files):
    df_currency = get_parquet('currency')
    dim_currency = create_dim_currency(df_currency)
    assert dim_currency.shape[1] == 3
    assert dim_currency['currency_code'][1] == 'BBB'

def test_dim_counterparty(mock_bucket_and_parquet_files):
    df_address = get_parquet('address')
    df_counterparty = get_parquet('counterparty')
    dim_counterparty = create_dim_counterparty(df_address, df_counterparty)
    assert dim_counterparty.shape[1] == 9
    assert dim_counterparty['counterparty_legal_name'][0] == 'cp-a'
    assert dim_counterparty['counterparty_legal_district'][1] == 'district-b'

def test_dim_staff(mock_bucket_and_parquet_files):
    df_staff = get_parquet('staff')
    df_department =get_parquet('department')
    dim_staff = create_dim_staff(df_staff, df_department)
    assert dim_staff.shape[1] == 6
    assert dim_staff['first_name'][2] == 'fn-c'
    assert dim_staff['department_name'][1] == 'dept-b'
    assert dim_staff['email_address'][0] == 'fna.lna@terrifictotes.com'

def test_fact_sales_order_table(mock_bucket_and_parquet_files):
    df_sales_order = get_parquet('sales_order')
    sales_order_table = create_facts_sales_order_table(df_sales_order)
    assert sales_order_table.shape[1] == 15
    assert sales_order_table['sales_record_id'][0] == 1
    assert sales_order_table['sales_record_id'][1] == 2
    assert sales_order_table['sales_order_id'][0] == 1
    assert sales_order_table['sales_order_id'][1] == 2
    assert sales_order_table['created_date'][0] == '2023-01-01'
    assert sales_order_table['created_time'][0] == '10:00:00'
    assert sales_order_table['last_updated_date'][0] == '2023-01-01'
    assert sales_order_table['last_updated_time'][0] == '10:00:00'
    assert sales_order_table['sales_staff_id'][1] == 2
    assert sales_order_table['counterparty_id'][1] == 2
    assert sales_order_table['units_sold'][0] == 10
    assert sales_order_table['unit_price'][1] == 2.00
    assert sales_order_table['agreed_delivery_date'][0] == '2023-01-01'
    assert sales_order_table['agreed_delivery_location_id'][4] == 5





