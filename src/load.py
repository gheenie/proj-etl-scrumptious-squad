import pandas as pd
from dotenv import load_dotenv
from os.path import join, dirname
import os
import pg8000
import boto3
from pathlib import Path
# import awswrangler


dotenv_path = join(dirname(__file__), '../config/.env')
load_dotenv(dotenv_path)

# Gives the access to the bucket data


def get_data(bucket):
    s3_client = boto3.client("s3")
    bucket = s3_client.list_objects_v2(Bucket = bucket)
    return bucket


# Make connection to data warehouse
def make_warehouse_connection():
    dotenv_path = Path('./config/.env.data_warehouse')
    load_dotenv(dotenv_path)
    API_HOST =  os.environ["host"]
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
