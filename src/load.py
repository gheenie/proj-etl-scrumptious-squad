import pandas as pd
from dotenv import load_dotenv
from os.path import join, dirname
import os
import pg8000
import boto3
from pathlib import Path
import io
import pyarrow.parquet as pq


dotenv_path = join(dirname(__file__), '../config/.env')
load_dotenv(dotenv_path)

# Gives the access to the bucket data



# Make connection to data warehouse
def make_warehouse_connection(dotenv_path="./config/.env.data_warehouse"):
    dotenv = Path(dotenv_path)
    load_dotenv(dotenv)
    API_HOST = os.environ["host"]
    API_USER = os.environ["user"]
    API_PASS = os.environ["password"]
    API_DBASE = os.environ["database"]
    conn = pg8000.connect(
        host=API_HOST,
        user=API_USER,
        password=API_PASS,
        database=API_DBASE
    )
    return conn


def get_data(bucket_name, file_path):
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_path)['Contents']
    dfs = {}
    for obj in objects:
        key = obj['Key']
        filename = key.split('/')[-1].split('.')[0]
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        buffer = io.BytesIO(obj['Body'].read())
        table = pq.read_table(buffer)
        df = table.to_pandas()
        dfs[f"df_{filename}"] = df
    return dfs

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
