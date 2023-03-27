import pandas as pd
import numpy as np
import boto3
from io import BytesIO
import os


def get_bucket_name(bucket_prefix):
    s3 = boto3.client('s3')
    response = s3.list_buckets()

    for bucket in response['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            return bucket['Name']


# Check if we are using secret manager, if so we need to retrive the secret key

# We need a function that will read the files from the "ingested data" s3 bucket

def get_parquet(title):
    bucketname = get_bucket_name('scrumptious-squad-pr-data-')
    s3 = boto3.client('s3')
    response =s3.list_objects_v2(Bucket=bucketname)
    filename = f"{title}.parquet"   

    if response['KeyCount'] == 0: return False

    if filename in [file['Key'] for file in response['Contents']]:       
        print(filename)    
        buffer = BytesIO()
        client = boto3.resource('s3')
        object=client.Object(bucketname, filename)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
        return df


# We need a function to check_data_files
# check the data is in the right format (readable parquet file) 
#       if corrupted or in wrong format dont execute 

# Not sure if the two functions mentioned above are combined in one?


#  This currently reads the csv files and puts the outcome in a Dataframe. Eventually this will need to be changed to read the parquet files from the aws s3 bucket
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


def create_dim_date(start_date, end_date):
    #using pandas date_range method
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    dim_date = pd.DataFrame(date_range, columns=['date_id'])
    dim_date['year'] = dim_date['date_id'].dt.year
    dim_date['month'] = dim_date['date_id'].dt.month
    dim_date['day'] = dim_date['date_id'].dt.day
    dim_date['day_of_week'] = dim_date['date_id'].dt.isocalendar().day
    dim_date['day_name'] = dim_date['date_id'].dt.day_name()
    dim_date['month_name'] = dim_date['date_id'].dt.strftime('%B')
    dim_date['quarter'] = dim_date['date_id'].dt.quarter
    
    return dim_date

def create_dim_location():
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

def create_dim_design():
    dim_design = pd.DataFrame() 
    dim_design['design_id'] = df_design['design_id']
    dim_design['design_name'] = df_design['design_name']
    dim_design['file_location'] = df_design['file_location']
    dim_design['file_name'] = df_design['file_name']

    return dim_design

def create_dim_currency():
    dim_currency = pd.DataFrame()
    dim_currency['currency_id'] = df_currency['currency_id']
    dim_currency['currency_code'] = df_currency['currency_code']
    conditions = [(dim_currency['currency_code'] == 'GBP'), dim_currency['currency_code'] == 'USD', dim_currency['currency_code'] == 'EUR']
    values = ['British Pound Sterling', 'United States Dollar', 'Euro']
    dim_currency['currency_name'] = np.select(conditions, values)
    
    return dim_currency
    
def create_dim_counterparty():
    dim_counterparty = pd.DataFrame()
    df_add = pd.merge(df_address,df_counterparty, left_on='address_id', right_on='legal_address_id')
    dim_counterparty['counterparty_id'] = df_counterparty['counterparty_id']
    dim_counterparty['counterparty_legal_name'] = df_counterparty['counterparty_legal_name']
    dim_counterparty['counterparty_legal_address_line_1'] = df_add['address_line_1']
    dim_counterparty['counterparty_legal_address_line2'] = df_add['address_line_2']
    dim_counterparty['counterparty_legal_district'] = df_add['district']
    dim_counterparty['counterparty_legal_city'] = df_add['city']
    dim_counterparty['counterparty_legal_postal_code'] = df_add['postal_code']
    dim_counterparty['counterparty_legal_country'] = df_add['country']
    dim_counterparty['counterparty_legal_phone_number'] = df_add['phone']
    
    return dim_counterparty


def create_dim_staff():
    dim_staff = pd.DataFrame()
    df_stf_dep = pd.merge(df_staff,df_department, on='department_id')
    dim_staff['staff_id'] = df_stf_dep['staff_id']
    dim_staff['first_name'] = df_stf_dep['first_name']
    dim_staff['last_name'] = df_stf_dep['last_name']
    dim_staff['department_name'] = df_stf_dep['department_name']
    dim_staff['location'] = df_stf_dep['location']
    dim_staff['email_address'] = df_stf_dep['email_address']
    dim_staff.sort_values('staff_id', inplace=True)
    
    return dim_staff


def create_facts_sales_order_table():
    sales_order_table = pd.DataFrame() 
    sales_order_table.insert(0, "sales_record_id", range(1, 1 + len(df_sales_order)))
    sales_order_table["sales_order_id"] = df_sales_order["sales_order_id"]
    sales_order_table[["created_date", "created_time"]] = df_sales_order["created_at"].apply(lambda x: pd.Series(str(x).split(" ")))
    sales_order_table[["last_updated_date", "last_updated_time"]] = df_sales_order["last_updated"].apply(lambda x: pd.Series(str(x).split(" ")))
    sales_order_table["sales_staff_id"] = df_sales_order["staff_id"]
    sales_order_table["counterparty_id"] = df_sales_order["counterparty_id"]
    sales_order_table["units_sold"] = df_sales_order["units_sold"]
    sales_order_table["unit_price"] = df_sales_order["unit_price"]
    sales_order_table["currency_id"] = df_sales_order["currency_id"]
    sales_order_table["design_id"] = df_sales_order["design_id"]
    sales_order_table["agreed_payment_date"] = df_sales_order["agreed_payment_date"]
    sales_order_table["agreed_delivery_date"] = df_sales_order["agreed_delivery_date"]
    sales_order_table["agreed_delivery_location_id"] = df_sales_order["agreed_delivery_location_id"]

    return sales_order_table


dim_date = {'dim_date': create_dim_date('2022-01-01', '2050-01-01')}
dim_location = {'dim_location' : create_dim_location()}
dim_design = {'dim_design' : create_dim_design()}
dim_currency = {'dim_currency' : create_dim_currency()}
dim_counterparty = {'dim_counterparty' : create_dim_counterparty()}
dim_staff = {'dim_staff' : create_dim_staff()}
facts_sales_order = {'facts_sales_order' : create_facts_sales_order_table()}


# We need a function that will put the files into the "processed data" s3 bucket

def push_to_cloud(object): 
        #seperate key and value from object              
        key = [key for key in object.keys()][0]
        values = object[key] 

        #use key for file name, and value as the content for the file       
        values.to_parquet(f'./database_access/data/parquet/{key}.parquet') 

        print(key)

        s3 = boto3.client('s3')
        get_bucket_name('scrumptious-squad-pr-data-')

        out_buffer = BytesIO()
        values.to_parquet(out_buffer, index=False, compression="gzip")

        s3.upload_file(f'./database_access/data/parquet/{key}.parquet', bucketname, f'{key}.parquet')
        os.remove(f'./database_access/data/parquet/{key}.parquet')        
       
     
        return True


push_to_cloud(dim_date)
push_to_cloud(dim_location)
push_to_cloud(dim_design)
push_to_cloud(dim_currency)
push_to_cloud(dim_counterparty)
push_to_cloud(dim_staff)
push_to_cloud(facts_sales_order)

