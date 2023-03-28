import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
import os
import pg8000
import boto3
# import awswrangler
from decouple import config


load_dotenv()


# # converts to human-readable format for error handling
def read_data(PROCESSED_DATA_BUCKET):
    df = {}
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(PROCESSED_DATA_BUCKET)

    for obj in bucket.objects.all():
        processed_s3_data = obj.get()['Body'].read()
        processed_s3_data[obj.key] = df
    return df


print(read_data("scrumptious-squad-pr-data-20230323093358336100000006"))

# s3 = boto3.resource('s3')
# bucket = s3.Bucket(PROCESSED_DATA_BUCKET)
# return pd.read_parquet(f"${parquet_path}", engine="auto")


# print(PROCESSED_DATA_BUCKET)


# def read_s3_data():
#     processed_s3_data = {}
#     df = awswrangler.s3.read_parquet(
#         path=f"s3://{PROCESSED_DATA_BUCKET}/", dataset=True)

#     processed_s3_data = df
#     return processed_s3_data


# Checks data is not corrupted
def corruption_checker():
    pass

# Make connection to data warehouse


def make_warehouse_connection():
    load_dotenv()
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
