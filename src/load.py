"""
Extracts data from the processed data bucket, convert into table format
and load to data warehouse
"""

import io
import json
import pyarrow.parquet as pq
import boto3
import pg8000
from botocore.exceptions import ClientError


def pull_secrets(secret_id):
    """
    Retrieves the secret from SecretManager
    """
    secret_manager = boto3.client("secretsmanager")
    try:
        response = secret_manager.get_secret_value(SecretId=secret_id)
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            raise ValueError(f"Secret id:{secret_id} doesn't exist") from error
        else:
            raise error
    secret_text = json.loads(response["SecretString"])
    return secret_text


def get_bucket_name(bucket_prefix):
    """
    Returns the name of the first S3 bucket that matches the given prefix
    Returns None if no matching bucket is found or an error occurs
    """
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_buckets()

        for bucket in response.get('Buckets', []):
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
    except ClientError as error:
        print(f"An error occurred: {error}")
    return None


def get_data(bucket_prefix):
    """
    Retrieves parquet files from the processed data bucket
    Returns the dictionary of the retrieved files
    """
    try:
        s3_client = boto3.client('s3')
        bucket_name = get_bucket_name(bucket_prefix)

        if not bucket_name:
            return []
        s3_client = boto3.client('s3')
        objects = s3_client.list_objects_v2(
            Bucket=bucket_name)['Contents']
        dfs = {}
        for obj in objects:
            key = obj['Key']
            filename = key.split('/')[-1].split('.')[0]
            obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            buffer = io.BytesIO(obj['Body'].read())
            table = pq.read_table(buffer)
            data_frame = table.to_pandas()
            dfs[f"data_frame_{filename}"] = data_frame
        return dfs

    except ClientError as error:
        print(f"An error occurred: {error}")
        return []


def make_warehouse_connection(secret_id):
    """
    Establish connection to a data warehouse
    """
    try:
        details = pull_secrets(secret_id)
        api_host = details['host']
        api_user = details['user']
        api_pass = details['password']
        api_dbase = details['database']
        conn = pg8000.connect(
            host=api_host,
            user=api_user,
            password=api_pass,
            database=api_dbase
        )
        return conn
    except Exception as error:
        print(f"Error connecting to the data warehouse: {str(error)}")
        return None


def load_data_to_warehouse(secret_id, bucket_prefix):
    """
    Converts data into tables format and loads it to
    the warehouse
    """
    try:
        conn = make_warehouse_connection(secret_id)
        if not conn:
            return False
        dfs = get_data(bucket_prefix)
        if not dfs:
            return False
        with conn.cursor() as cursor:
            for table in dfs:
                table_name = table[3:]
                print(f"Loading table {table_name}")
                for row in dfs[table].itertuples(index=False):
                    values = ', '.join(['%s'] * len(row))
                    columns = ', '.join(row._fields)
                    sql = f"INSERT INTO{table_name}({columns})VALUES({values})"
                    cursor.execute(sql, row)
                    conn.commit()
                print(f"Data loaded into table {table_name}")
        cursor.close()
        conn.close()
        return {
            'statusCode': 200,
            'body': 'Successfully loaded into data warehouse'
        }
    except Exception as error:
        print(f"Error loading data into the data warehouse: {str(error)}")
        return False


def load_lambda_handler(event, context):
    """
    Fully integrated all subfunctions
    """
    try:
        # Retrieve the secret ID and bucket prefix from the event
        secret_id = event.get('secret_id')
        bucket_prefix = event.get('bucket_prefix')
        # Load data from S3 bucket
        data = get_data(bucket_prefix)
        if not data:
            return {
                'statusCode': 400,
                'body': 'Error: Failed to load data from S3 bucket'
            }
        result = load_data_to_warehouse(secret_id, bucket_prefix)
        if not result:
            return {
                'statusCode': 400,
                'body': 'Error: Failed to load data into warehouse'
            }

        return {
            'statusCode': 200,
            'body': 'Data loaded into warehouse successfully'
        }

    except Exception as error:
        return {
            'statusCode': 500,
            'body': f"Error: {str(error)}"
        }
