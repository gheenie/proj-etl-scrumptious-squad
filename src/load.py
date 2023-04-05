"""
Extracts data from the processed data bucket, convert into table format
and load to data warehouse
"""

import io
import json
import logging
import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

logger = logging.getLogger('mylogger')
logger.setLevel(logging.INFO)


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
            dfs[f"df_{filename}"] = data_frame
        return dfs

    except ClientError as error:
        print(f"An error occurred: {error}")
        return []


def load_data_to_warehouse(secret_id, bucket_prefix):
    try:
        dfs = get_data(bucket_prefix)
        if not dfs:
            return False
        # Pulls secrets but doesn't connect to the warehouse yet
        details = pull_secrets(secret_id)
        host = details['host']
        user = details['user']
        pword = details['password']
        dbase = details['database']
        schema = details['schema']
        # Specifies postgreSQL as the database, then its config
        conn_string = f'postgresql://{user}:{pword}@{host}/{dbase}'
        db_engine = create_engine(conn_string)

        for table in dfs:
            table_name = table[3:]
            logger.info(f"Loading table {table_name}")
            table_as_dataframe = dfs[table]
            # logger.debug(f"DataFrame for {table_name}: {table_as_dataframe}")
            table_as_dataframe.to_sql(
                table_name,
                schema=schema,
                con=db_engine,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi'
            )
        logger.info("Successfully loaded data into the data warehouse")
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
