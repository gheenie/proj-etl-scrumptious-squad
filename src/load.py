from dotenv import load_dotenv
import os
import pg8000
import boto3
from pathlib import Path
import pyarrow.parquet as pq
import io
import logging
import json
from botocore.exceptions import ClientError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def pull_secrets():
    secret_name = 'cred_DW'     
    secrets_manager = boto3.client('secretsmanager')

    try:               
        response = secrets_manager.get_secret_value(SecretId=secret_name)  

    except ClientError as e:            
        error_code = e.response['Error']['Code']

        print(error_code)
        if error_code == 'ResourceNotFoundException':            
            raise Exception(f'ERROR: name not found') 
        else:           
            raise Exception(f'ERROR : {error_code}')
    else:
        secrets = json.loads(response['SecretString'])
        details = {
        'user': secrets['user'][0],
        'password': secrets['password'][0],
        'database': secrets['database'][0],
        'host':secrets['host'][0],
        'port':secrets['port'],
        'schema': secrets['schema']
        }
        return details['user'], details['password'], details['database'], details['host'], details['port'], details['schema']


def get_bucket_name(bucket_prefix):
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        for bucket in response['Buckets']:
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_data(bucket_prefix):
    bucket_name = get_bucket_name(bucket_prefix)
    file_path = 'data/parquet'
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=file_path)['Contents']
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

def make_warehouse_connection():
    try:
        details = pull_secrets()
        API_HOST = details['host']
        API_USER = details['user']
        API_PASS = details["password"]
        API_DBASE = details["database"]
        conn = pg8000.connect(
            host=API_HOST,
            user=API_USER,
            password=API_PASS,
            database=API_DBASE
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the data warehouse: {str(e)}")
        return None


def load_to_warehouse(conn, dfs):
    cur = conn.cursor()
    for table in dfs:
        table_name = table[3:]
        print(f"Loading table {table_name}")
        for index, row in dfs[table].iterrows():
            values = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cur.execute(sql, tuple(row))
    conn.commit()
    cur.close()
    conn.close()
    return {
        'statusCode': 200,
        'body': 'Successfully loaded into data warehouse'
    }


def load_lambda_handler(event, context):
    bucket_name = event['bucket_name']
    file_path = event['file_path']
    dotenv_path = event['dotenv_path']

    try:
        dfs = get_data()
        conn = make_warehouse_connection()
        if conn is None:
            return {
                'statusCode': 500,
                'body': 'Error: Unable to connect to the data warehouse'
            }
        logger.info(f'Bucket is {bucket_name}')
        return load_to_warehouse(conn=conn,dfs=dfs)
    except pg8000.core.DatabaseError as e:
        print(f"Error Database does not exist: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error loading data to the warehouse: {str(e)}'
        }
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f'No such bucket - {bucket_name}')
        else:
            raise
    except Exception as e:
        logger.error(e)
        raise RuntimeError
