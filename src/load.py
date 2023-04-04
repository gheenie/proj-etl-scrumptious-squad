import pg8000
import boto3
import pyarrow.parquet as pq
import io
import json
from botocore.exceptions import ClientError

def pull_secrets(secret_id):
    secret_manager = boto3.client("secretsmanager")
    try:
        response = secret_manager.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise ValueError(f"Secret with {secret_id} does not exist")
        else:
            raise e
    secret_text = json.loads(response["SecretString"])
    return secret_text

def get_bucket_name(bucket_prefix):
    """
    Returns the name of the first S3 bucket that matches the given prefix.
    Returns None if no matching bucket is found or an error occurs.
    """
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()

        for bucket in response.get('Buckets', []):
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
    except ClientError as e:
        print(f"An error occurred: {e}")

    return None


def get_data(bucket_prefix):
    try:
        s3 = boto3.client('s3')
        bucket_name = get_bucket_name(bucket_prefix)

        if not bucket_name:
            return []
        s3 = boto3.client('s3')
        objects = s3.list_objects_v2(
            Bucket=bucket_name)['Contents']
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

    except ClientError as e:
        print(f"An error occurred: {e}")
        return []

def make_warehouse_connection(secret_id):
    try:
        details = pull_secrets(secret_id)
        API_HOST = details['host']
        API_USER = details['user']
        API_PASS = details['password']
        API_DBASE = details['database']
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
    

from sqlalchemy import create_engine
def load_data_to_warehouse(secret_id, bucket_prefix):
    try:
        # conn = make_warehouse_connection(secret_id)
        # if not conn:
        #     return False
        
        dfs = get_data(bucket_prefix)
        if not dfs:
            return False

        details = pull_secrets(secret_id)
        API_HOST = details['host']
        API_USER = details['user']
        API_PASS = details['password']
        API_DBASE = details['database']
        conn_string = f'postgresql://{API_USER}:{API_PASS}@{API_HOST}/{API_DBASE}'
        db = create_engine(conn_string)
        conn = db.connect()

        for table in dfs:
            table_name = table[3:]
            print(f"Loading table {table_name}")
            table_as_dataframe = dfs[table]
            table_as_dataframe.to_sql(table_name, con=conn, if_exists='append', index=False)
        
        # with conn.cursor() as cursor:
        #     for table in dfs:
        #         table_name = table[3:]
        #         print(f"Loading table {table_name}")
        #         for row in dfs[table].itertuples(index=False):
        #             values = ', '.join(['%s'] * len(row))
        #             columns = ', '.join(row._fields)
        #             sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        #             cursor.execute(sql, row)
        #             conn.commit()
        #         print(f"Data loaded into table {table_name}")
                
        # cursor.close()
        # conn.close()
        return {
            'statusCode': 200,
            'body': 'Successfully loaded into data warehouse'
        }
        
    except Exception as e:
        print(f"Error loading data into the data warehouse: {str(e)}")
        return False


def load_lambda_handler(event, context):
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

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
