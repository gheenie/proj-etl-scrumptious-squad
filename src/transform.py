"""
the transform function will change the schema of the data
turning it into fact and dim tables
"""

from io import BytesIO
import os
import boto3
import numpy as np
import pandas as pd


def get_bucket_name(bucket_prefix):
    """
    Get the right bucket
    """
    s3_client = boto3.client('s3')
    response = s3_client.list_buckets()

    for bucket in response['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            return bucket['Name']


def get_parquet(title):
    """
    Get files from the bucket
    """
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucketname)
    filename = f"{title}.parquet"

    if response['KeyCount'] == 0:
        return False

    if filename in [file['Key'] for file in response['Contents']]:
        buffer = BytesIO()
        client = boto3.resource('s3')
        client_object = client.Object(bucketname, filename)
        client_object.download_fileobj(buffer)
        data_frame = pd.read_parquet(buffer)
        return data_frame


def create_dim_date(start_date, end_date):
    """
    Create dim_date using pandas date_range method
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    dim_date = pd.DataFrame(date_range, columns=['date_id'])
    dim_date['year'] = dim_date['date_id'].dt.year.astype('int64')
    dim_date['month'] = dim_date['date_id'].dt.month.astype('int64')
    dim_date['day'] = dim_date['date_id'].dt.day.astype('int64')
    dim_date['day_of_week'] = dim_date['date_id'].dt.isocalendar().day
    dim_date['day_of_week'] = pd.to_numeric(
        dim_date['day_of_week'], errors="coerce").astype('int64')
    dim_date['day_name'] = dim_date['date_id'].dt.day_name()
    dim_date['month_name'] = dim_date['date_id'].dt.strftime('%B')
    dim_date['quarter'] = dim_date['date_id'].dt.quarter.astype('int64')
    return dim_date


def create_dim_location(df_address):
    """
    Create dim_location
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
    Create dim_design
    """
    dim_design = pd.DataFrame()
    dim_design['design_id'] = df_design['design_id']
    dim_design['design_name'] = df_design['design_name']
    dim_design['file_location'] = df_design['file_location']
    dim_design['file_name'] = df_design['file_name']
    return dim_design


def create_dim_currency(df_currency):
    """
    Create dim_currency
    """
    dim_currency = pd.DataFrame()
    dim_currency['currency_id'] = df_currency['currency_id']
    dim_currency['currency_code'] = df_currency['currency_code']
    condition_1 = dim_currency['currency_code'] == 'GBP'
    condition_2 = dim_currency['currency_code'] == 'USD'
    condition_3 = dim_currency['currency_code'] == 'EUR'
    conditions = [condition_1, condition_2, condition_3]
    values = ['British Pound Sterling', 'United States Dollar', 'Euro']
    dim_currency['currency_name'] = np.select(conditions, values)
    return dim_currency


def create_dim_counterparty(df_a, df_c):
    """
    Create dim_counterparty
    """
    dim_cp = pd.DataFrame()
    a = pd.merge(df_a, df_c, left_on='address_id', right_on='legal_address_id')
    dim_cp['counterparty_id'] = df_c['counterparty_id']
    dim_cp['counterparty_legal_name'] = df_c['counterparty_legal_name']
    dim_cp['counterparty_legal_address_line_1'] = a['address_line_1']
    dim_cp['counterparty_legal_address_line_2'] = a['address_line_2']
    dim_cp['counterparty_legal_district'] = a['district']
    dim_cp['counterparty_legal_city'] = a['city']
    dim_cp['counterparty_legal_postal_code'] = a['postal_code']
    dim_cp['counterparty_legal_country'] = a['country']
    dim_cp['counterparty_legal_phone_number'] = a['phone']
    return dim_cp


def create_dim_staff(df_staff, df_department):
    """
    Create dim_staff
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
    Create dim_transaction
    """
    dim_transaction = pd.DataFrame()
    dim_transaction['transaction_id'] = df_transaction['transaction_id']
    dim_transaction['transaction_type'] = df_transaction['transaction_type']
    dim_transaction['sales_order_id'] = df_transaction['sales_order_id']
    dim_transaction['purchase_order_id'] = df_transaction['purchase_order_id']
    return dim_transaction


def create_dim_payment_type(df_payment_type):
    """
    Create dim_payment_type
    """
    dim_pt = pd.DataFrame()
    dim_pt['payment_type_id'] = df_payment_type['payment_type_id']
    dim_pt['payment_type_name'] = df_payment_type['payment_type_name']
    return dim_pt


def create_fact_sales_order(df_s):
    """
    Create fact_sales_order
    """
    fact_s = pd.DataFrame()
    # fact_s.insert(0, "sales_record_id", range(1, 1 + len(df_s)))
    fact_s["sales_order_id"] = df_s["sales_order_id"]
    fact_s[["created_date", "created_time"]] = df_s["created_at"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    str1 = "last_updated_date"
    str2 = "last_updated_time"
    fact_s[[str1, str2]] = df_s["last_updated"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    fact_s["sales_staff_id"] = df_s["staff_id"]
    fact_s["counterparty_id"] = df_s["counterparty_id"]
    fact_s["units_sold"] = df_s["units_sold"]
    fact_s["unit price"] = df_s["unit_price"]
    fact_s["currency_id"] = df_s["currency_id"]
    fact_s["design_id"] = df_s["design_id"]
    fact_s["agreed_payment_date"] = df_s["agreed_payment_date"]
    fact_s["agreed_delivery_date"] = df_s["agreed_delivery_date"]
    fact_s["agreed_delivery_location_id"] = df_s["agreed_delivery_location_id"]
    return fact_s


def create_fact_purchase_order(df_p):
    """
    Create fact_purchase_order
    """
    fact_p = pd.DataFrame()
    # fact_p.insert(
    #     0, "purchase_record_id", range(1, 1 + len(df_p)))
    fact_p["purchase_order_id"] = df_p["purchase_order_id"]
    fact_p[["created_date", "created_time"]] = df_p["created_at"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    str1 = "last_updated_date"
    str2 = "last_updated_time"
    fact_p[[str1, str2]] = df_p["last_updated"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    fact_p["staff_id"] = df_p["staff_id"]
    fact_p["counterparty_id"] = df_p["counterparty_id"]
    fact_p["item_code"] = df_p["item_code"]
    fact_p["item_quantity"] = df_p["item_quantity"]
    fact_p["item_unit_price"] = df_p["item_unit_price"]
    fact_p["currency_id"] = df_p["currency_id"]
    fact_p["agreed_delivery_date"] = df_p["agreed_delivery_date"]
    fact_p["agreed_payment_date"] = df_p["agreed_payment_date"]
    fact_p["agreed_delivery_location_id"] = df_p["agreed_delivery_location_id"]
    return fact_p


def create_fact_payment(df_pay):
    """
    Create fact_payment
    """
    fact_pay = pd.DataFrame()
    # fact_pay.insert(0, "payment_record_id", range(1, 1 + len(df_pay)))
    fact_pay["payment_id"] = df_pay["payment_id"]
    fact_pay[["created_date", "created_time"]] = df_pay["created_at"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    str1 = "last_updated_date"
    str2 = "last_updated"
    fact_pay[[str1, str2]] = df_pay["last_updated"].apply(
        lambda x: pd.Series(str(x).split(" ")))
    fact_pay["transaction_id"] = df_pay["transaction_id"]
    fact_pay["counterparty_id"] = df_pay["counterparty_id"]
    fact_pay["payment_amount"] = df_pay["payment_amount"]
    fact_pay["currency_id"] = df_pay["currency_id"]
    fact_pay["payment_type_id"] = df_pay["payment_type_id"]
    fact_pay["paid"] = df_pay["paid"]
    fact_pay["payment_date"] = df_pay["payment_date"]
    return fact_pay


def push_to_cloud(local_object):
    """
    Uploads the files to the processed data s3 bucket
    """
    # seperate key and value from object
    key = [key for key in local_object.keys()][0]
    values = local_object[key]
    # use key for file name, and value as the content for the file
    values.to_parquet(f'/tmp/{key}.parquet')
    s3_client = boto3.client('s3')
    bucket_name = get_bucket_name('scrumptious-squad-pr-data-')
    out_buffer = BytesIO()
    values.to_parquet(out_buffer, index=False, compression="gzip")
    s3_client.upload_file(f'/tmp/{key}.parquet', bucket_name, f'{key}.parquet')
    os.remove(f'/tmp/{key}.parquet')
    return True


def transform():
    """
    Read the parquet files from the s3 bucket
    and put the outcome in a Dataframe.
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

    """
    Converts dataframes to dictionaries
    """
    dim_date = {'dim_date': create_dim_date('2022-01-01', '2024-01-01')}
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
    fact_purchase_orders = {
        'fact_purchase_order': create_fact_purchase_order(df_purchase_order)}
    fact_payment = {'fact_payment': create_fact_payment(df_payment)}

    """
    Uploads each dataframe into a parquet file
    and uploads that file into the processed data s3 bucket
    """

    push_to_cloud(dim_date)
    push_to_cloud(dim_location)
    push_to_cloud(dim_design)
    push_to_cloud(dim_currency)
    push_to_cloud(dim_counterparty)
    push_to_cloud(dim_staff)
    push_to_cloud(dim_transaction)
    push_to_cloud(dim_payment_type)
    push_to_cloud(fact_sales_order)
    push_to_cloud(fact_purchase_orders)
    push_to_cloud(fact_payment)

# transform()


# Lambda handler
def transform_lambda_handler(event, context):
    """
    Fully integrated all subfunctions
    """

    transform()
    # logger.info("Completed")
