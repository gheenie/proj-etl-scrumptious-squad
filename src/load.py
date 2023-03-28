import pandas as pd
from dotenv import load_dotenv
from os.path import join, dirname
import os
import pg8000
import boto3
# import awswrangler


dotenv_path = join(dirname(__file__), '../config/.env')
load_dotenv(dotenv_path)

# Gives the access to the bucket data


def read_data(PROCESSED_DATA_BUCKET):
    df = {}
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(PROCESSED_DATA_BUCKET)
    for obj in s3.buckets.all():
        df = obj.get_available_subresources()
    return df


# Make connection to data warehouse
def make_warehouse_connection():
    dotenv_path = join(dirname(__file__), '../config/.env.data_warehouse')
    load_dotenv(dotenv_path)
    API_HOST = config('host')
    API_USER = config('user')
    API_PASS = config('password')
    API_DBASE = config('database')
    conn = pg8000.connect(
        host=API_HOST,
        user=API_USER,
        password=API_PASS,
        database=API_DBASE
    )
    return conn


print(make_warehouse_connection())

# Pushes parqueted data to data warehouse


def insert_into_warehouse():
    pass

# Integrated function to combine all of the above


def load_to_warehouse():
    try:
        # Log status message to confirm load
        pass
    except:
        # Log status with helpful error message
        pass

# Lambda_handler


def lambda_handler():
    pass
