from dotenv import load_dotenv
import os
import pg8000
import boto3
from pathlib import Path
import io
import pyarrow.parquet as pq
import io
import pyarrow.parquet as pq


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


def make_warehouse_connection(dotenv_path="./config/.env.data_warehouse"):
    try:
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
    except Exception as e:
        print(f"Error connecting to the data warehouse: {str(e)}")
        return None


def load_to_warehouse(conn, dfs):
    try:
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
        return "Successfully loaded into data warehouse"
    except Exception as e:
        print(f"Error loading data to the warehouse: {str(e)}")
        return None


def load_lambda_handler(event, context):
    bucket_name = event['bucket_name']
    file_path = event['file_path']
    dotenv_path = event['dotenv_path']

    try:
        dfs = get_data(bucket_name, file_path)
        conn = make_warehouse_connection(dotenv_path)
        if conn is None:
            return {
                'statusCode': 500,
                'body': 'Error: Unable to connect to the data warehouse'
            }
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
    except Exception as e:
        print(f"Error loading data to the warehouse: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error loading data to the warehouse: {str(e)}'
        }