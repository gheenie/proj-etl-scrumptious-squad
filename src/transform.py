"""
aaa
"""

from io import BytesIO
import os
import boto3
import numpy as np
import pandas as pd


#  Get the right bucket
def get_bucket_name(bucket_prefix):
    """
    aaa
    """
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()

    for bucket in response['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            return bucket['Name']


# Get files from the bucket
def get_parquet(title):
    """
    hey there
    """
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucketname)
    filename = f"{title}.parquet"

    if response['KeyCount'] == 0:
        return False

    if filename in [file['Key'] for file in response['Contents']]:
        # print(filename)
        buffer = BytesIO()
        client = boto3.resource('s3')
        client_object = client.Object(bucketname, filename)
        client_object.download_fileobj(buffer)
        data_frame = pd.read_parquet(buffer)
        return data_frame


def create_dim_date(start_date, end_date):
    """
    aaa
    """
    # using pandas date_range method
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    dim_date = pd.DataFrame(date_range, columns=['date_id'])
    dim_date['year'] = dim_date['date_id'].dt.year
    dim_date['month'] = dim_date['date_id'].dt.month
    dim_date['day'] = dim_date['date_id'].dt.day
    dim_date['day_of_week'] = dim_date['date_id'].dt.isocalendar().day
    dim_date['day_of_week'] = pd.to_numeric(
        dim_date['day_of_week'], errors="coerce").astype('int64')
    dim_date['day_name'] = dim_date['date_id'].dt.day_name()
    dim_date['month_name'] = dim_date['date_id'].dt.strftime('%B')
    dim_date['quarter'] = dim_date['date_id'].dt.quarter
    return dim_date


def create_dim_location(df_address):
    """
    aaa
    """
    dim_location = pd.DataFrame()
    dim_location['location_id'] = df_address['address_id']
    dim_location['address_line_1'] = df_address['address_line_1']
    dim_location['address_line_2'] = df_address['address_line_2']
    dim_location['district'] = df_address['district']
    dim_location['city'] = df_address['city']
    dim_location['postal_code'] = df_address['postal_code']
    dim_location['country'] = df_address['country']
    dim_location['phone'] = df_address['phone']
    return dim_location


def create_dim_design(df_design):
    """
    aaa
    """
    dim_design = pd.DataFrame()
    dim_design['design_id'] = df_design['design_id']
    dim_design['design_name'] = df_design['design_name']
    dim_design['file_location'] = df_design['file_location']
    dim_design['file_name'] = df_design['file_name']
    return dim_design


def create_dim_currency(df_currency):
    """
    aaa
    """
    dim_currency = pd.DataFrame()
    dim_currency['currency_id'] = df_currency['currency_id']
    dim_currency['currency_code'] = df_currency['currency_code']
    conditions = [(dim_currency['currency_code'] == 'GBP'), dim_currency['currency_code'] == 'USD', dim_currency['currency_code'] == 'EUR']
    values = ['British Pound Sterling', 'United States Dollar', 'Euro']
    dim_currency['currency_name'] = np.select(conditions, values)
    return dim_currency


def create_dim_counterparty(df_address, df_counterparty):
    """
    aaa
    """
    dim_counterparty = pd.DataFrame()
    df_add = pd.merge(df_address, df_counterparty, left_on='address_id', right_on='legal_address_id')
    dim_counterparty['counterparty_id'] = df_counterparty['counterparty_id']
    dim_counterparty['counterparty_legal_name'] = df_counterparty['counterparty_legal_name']
    dim_counterparty['counterparty_legal_address_line_1'] = df_add['address_line_1']
    dim_counterparty['counterparty_legal_address_line_2'] = df_add['address_line_2']
    dim_counterparty['counterparty_legal_district'] = df_add['district']
    dim_counterparty['counterparty_legal_city'] = df_add['city']
    dim_counterparty['counterparty_legal_postal_code'] = df_add['postal_code']
    dim_counterparty['counterparty_legal_country'] = df_add['country']
    dim_counterparty['counterparty_legal_phone_number'] = df_add['phone']
    return dim_counterparty


def create_dim_staff(df_staff, df_department):
    """
    aaa
    """
    dim_staff = pd.DataFrame()
    df_stf_dep = pd.merge(df_staff, df_department, on='department_id')
    dim_staff['staff_id'] = df_stf_dep['staff_id']
    dim_staff['first_name'] = df_stf_dep['first_name']
    dim_staff['last_name'] = df_stf_dep['last_name']
    dim_staff['department_name'] = df_stf_dep['department_name']
    dim_staff['location'] = df_stf_dep['location']
    dim_staff['email_address'] = df_stf_dep['email_address']
    dim_staff.sort_values('staff_id', inplace=True)
    return dim_staff


def create_dim_transaction(df_transaction):
    """
    aaa
    """
    dim_transaction = pd.DataFrame()
    dim_transaction['transaction_id'] = df_transaction['transaction_id']
    dim_transaction['transaction_type'] = df_transaction['transaction_type']
    dim_transaction['sales_order_id'] = df_transaction['sales_order_id']
    dim_transaction['purchase_order_id'] = df_transaction['purchase_order_id']
    return dim_transaction


def create_dim_payment_type(df_payment_type):
    """
    aaa
    """
    dim_payment_type = pd.DataFrame()
    dim_payment_type['payment_type_id'] = df_payment_type['payment_type_id']
    dim_payment_type['payment_type_name'] = df_payment_type['payment_type_name']
    return dim_payment_type


def create_fact_sales_order(df_sales_order):
    """
    aaa
    """
    fact_sales_order = pd.DataFrame()
    fact_sales_order.insert(0, "sales_record_id",
                            range(1, 1 + len(df_sales_order)))
    fact_sales_order["sales_order_id"] = df_sales_order["sales_order_id"]
    fact_sales_order[["created_date", "created_time"]] = df_sales_order["created_at"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    fact_sales_order[["last_updated_date", "last_updated_time"]
                     ] = df_sales_order["last_updated"].apply(lambda x: pd.Series(str(x).split(" ")))
    fact_sales_order["sales_staff_id"] = df_sales_order["staff_id"]
    fact_sales_order["counterparty_id"] = df_sales_order["counterparty_id"]
    fact_sales_order["units_sold"] = df_sales_order["units_sold"]
    fact_sales_order["unit_price"] = df_sales_order["unit_price"]
    fact_sales_order["currency_id"] = df_sales_order["currency_id"]
    fact_sales_order["design_id"] = df_sales_order["design_id"]
    fact_sales_order["agreed_payment_date"] = df_sales_order["agreed_payment_date"]
    fact_sales_order["agreed_delivery_date"] = df_sales_order["agreed_delivery_date"]
    fact_sales_order["agreed_delivery_location_id"] = df_sales_order["agreed_delivery_location_id"]
    return fact_sales_order


def create_fact_purchase_order(df_purchase_order):
    """
    aaa
    """
    fact_purchase_order = pd.DataFrame()
    fact_purchase_order.insert(
        0, "purchase_record_id", range(1, 1 + len(df_purchase_order)))
    fact_purchase_order["purchase_order_id"] = df_purchase_order["purchase_order_id"]
    fact_purchase_order[["created_date", "created_time"]] = df_purchase_order["created_at"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    fact_purchase_order[["last_updated_date", "last_updated_time"]
                        ] = df_purchase_order["last_updated"].apply(lambda x: pd.Series(str(x).split(" ")))
    fact_purchase_order["staff_id"] = df_purchase_order["staff_id"]
    fact_purchase_order["counterparty_id"] = df_purchase_order["counterparty_id"]
    fact_purchase_order["item_code"] = df_purchase_order["item_code"]
    fact_purchase_order["item_quantity"] = df_purchase_order["item_quantity"]
    fact_purchase_order["item_unit_price"] = df_purchase_order["item_unit_price"]
    fact_purchase_order["currency_id"] = df_purchase_order["currency_id"]
    fact_purchase_order["agreed_delivery_date"] = df_purchase_order["agreed_delivery_date"]
    fact_purchase_order["agreed_payment_date"] = df_purchase_order["agreed_payment_date"]
    fact_purchase_order["agreed_delivery_location_id"] = df_purchase_order["agreed_delivery_location_id"]
    return fact_purchase_order


def create_fact_payment(df_payment):
    """
    aaa
    """
    fact_payment = pd.DataFrame()
    fact_payment.insert(0, "payment_record_id", range(1, 1 + len(df_payment)))
    fact_payment["payment_id"] = df_payment["payment_id"]
    fact_payment[["created_date", "created_time"]] = df_payment["created_at"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    fact_payment[["last_updated_date", "last_updated_time"]
                 ] = df_payment["last_updated"].apply(lambda x: pd.Series(str(x).split(" ")))
    fact_payment["transaction_id"] = df_payment["transaction_id"]
    fact_payment["counterparty_id"] = df_payment["counterparty_id"]
    fact_payment["payment_amount"] = df_payment["payment_amount"]
    fact_payment["currency_id"] = df_payment["currency_id"]
    fact_payment["payment_type_id"] = df_payment["payment_type_id"]
    fact_payment["paid"] = df_payment["paid"]
    fact_payment["payment_date"] = df_payment["payment_date"]
    return fact_payment


def push_to_cloud(local_object):
    """
    aaa
    """
    # seperate key and value from object
    key = [key for key in local_object.keys()][0]
    values = local_object[key]
    # use key for file name, and value as the content for the file
    values.to_parquet(f'/tmp/{key}.parquet')
    # print(key)
    s3_client = boto3.client('s3')
    bucket_name = get_bucket_name('scrumptious-squad-pr-data-')
    out_buffer = BytesIO()
    values.to_parquet(out_buffer, index=False, compression="gzip")
    s3_client.upload_file(f'/tmp/{key}.parquet', bucket_name, f'{key}.parquet')
    os.remove(f'/tmp/{key}.parquet')
    return True


def transform():
    #  Read the parquet files from the s3 bucket
    #  and put the outcome in a Dataframe.
    """
    aaa
    """
    df_address = get_parquet('address')
    df_counterparty = get_parquet('counterparty')
    df_currency = get_parquet('currency')
    df_department = get_parquet('department')
    df_design = get_parquet('design')
    df_payment_type = get_parquet('payment_type')
    df_payment = get_parquet('payment')
    df_purchase_order = get_parquet('purchase_order')
    df_sales_order = get_parquet('sales_order')
    df_staff = get_parquet('staff')
    df_transaction = get_parquet('transaction')

    # Converts dataframes to dictionaries
    dim_date = {'dim_date': create_dim_date('2022-01-01', '2050-01-01')}
    dim_location = {'dim_location': create_dim_location(df_address)}
    dim_design = {'dim_design': create_dim_design(df_design)}
    dim_currency = {'dim_currency': create_dim_currency(df_currency)}
    dim_counterparty = {'dim_counterparty': create_dim_counterparty(
        df_address, df_counterparty)}
    dim_staff = {'dim_staff': create_dim_staff(df_staff, df_department)}
    dim_transaction = {
        'dim_transaction': create_dim_transaction(df_transaction)}
    dim_payment_type = {
        'dim_payment_type': create_dim_payment_type(df_payment_type)}
    fact_sales_order = {
        'fact_sales_order': create_fact_sales_order(df_sales_order)}
    fact_purchase_order = {
        'fact_purchase_order': create_fact_purchase_order(df_purchase_order)}
    fact_payment = {'fact_payment': create_fact_payment(df_payment)}

    push_to_cloud(dim_date)
    push_to_cloud(dim_location)
    push_to_cloud(dim_design)
    push_to_cloud(dim_currency)
    push_to_cloud(dim_counterparty)
    push_to_cloud(dim_staff)
    push_to_cloud(dim_transaction)
    push_to_cloud(dim_payment_type)
    push_to_cloud(fact_sales_order)
    push_to_cloud(fact_purchase_order)
    push_to_cloud(fact_payment)

# transform()

# Lambda handler


def something(event, context):
    """
    Fully integrated all subfunctions
    """
    transform()
    # logger.info("Completed")
