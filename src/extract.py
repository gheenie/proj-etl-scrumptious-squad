"""
the exreact function will get data updates from the data lake
and push it to the ingested data s3 bucket in parquet format
"""

import bisect
import json
import logging
from io import BytesIO
import os
from pathlib import Path
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import boto3
import pandas as pd
import pg8000
import pg8000.native

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def pull_secrets(secret_id="source_DB"):
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
            raise Exception(f'ERROR : {error}')
    else:
        secrets = json.loads(response['SecretString'])
        details = {
            'user': secrets['user'],
            'password': secrets['password'],
            'database': secrets['database'],
            'host': secrets['host'],
            'port': secrets['port']
        }
        return (
            details['database'],
            details['user'],
            details['password'],
            details['host'],
            details['port']
        )


def make_connection(dotenv_path_string):
    """
   Creates link to the data lake
    """
    dotenv_path = Path(dotenv_path_string)
    load_dotenv(dotenv_path=dotenv_path)

    if dotenv_path_string.endswith('development'):
        details = pull_secrets()
        conn = pg8000.connect(
            database=details['database'],
            user=details['user'],
            password=details['password'],
            host=details['host'],
            port=details['port'])
    elif dotenv_path_string.endswith('test'):
        conn = pg8000.connect(
            database=os.getenv('database'),
            user=os.getenv('user'),
            password=os.getenv('password')
        )
    return conn


def get_titles(dbcur):
    """
    Retrieves table names from the data lake
    """
    sql = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type= 'BASE TABLE';"""
    try:
        dbcur.execute(sql)
        return dbcur.fetchall()
    except Exception as error:
        raise Exception(f"ERROR FETCHING TITLES: {error}") from error


def get_whole_table(dbcur, title):
    """
    Retrieves content of each table in the data lake
    """
    sql = f'SELECT * FROM {title[0]}'
    try:
        dbcur.execute(sql)
        rows = dbcur.fetchall()
        keys = [k[0] for k in dbcur.description]
        return rows, keys
    except Exception as error:
        raise Exception(f"ERROR FETCHING TABLE {title[0]}: {error}") from error


def get_recents_table(dbcur, title, created, updated):
    """
    Identifies the newly added data and returns in table format
    """
    first_con = f"(created_at > '{created}'::timestamp)"
    second_con = f"(last_updated > '{updated}'::timestamp)"
    sql = f"SELECT * FROM {title[0]} WHERE ({first_con}) OR ({second_con})"
    try:
        dbcur.execute(sql)
        rows = dbcur.fetchall()
        keys = [k[0] for k in dbcur.description]
        return rows, keys
    except Exception as error:
        raise Exception(f"ERROR FETCHING TABLE {title[0]}: {error}") from error


def get_file_info_in_bucket(bucketname):
    """
    Returns the names of the files in the s3 bucket in JSON format
    """
    try:
        s3_client = boto3.client('s3')
        return s3_client.list_objects_v2(Bucket=bucketname)
    except Exception as error:
        raise Exception(f"ERROR CHECKING BUCKET OBJECTS: {error}") from error


def get_bucket_name(bucket_prefix):
    """
    Access S3 and returns the ingested bucket name
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_buckets()
    except Exception as error:
        raise Exception(f"ERROR FETCHING BUCKET NAME: {error}") from error

    for bucket in response['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            return bucket['Name']


def check_table_in_bucket(title, response):
    """
    Checks if the specified table exists in the S3 bucket
    """
    if response['KeyCount'] == 0:
        return False
    filename = f"{title[0]}.parquet"
    filenames = [file['Key'] for file in response['Contents']]
    return filename in filenames


def get_parquet(title, bucketname, response):
    """
    Retrieves the specified file and converts to a parquet format
    """
    filename = f"{title}.parquet"
    if response['KeyCount'] == 0:
        return False
    if filename in [file['Key'] for file in response['Contents']]:

        buffer = BytesIO()
        client = boto3.resource('s3')
        client_object = client.Object(bucketname, filename)
        client_object.download_fileobj(buffer)
        data_frame = pd.read_parquet(buffer)
        return data_frame


def get_most_recent_time(title, bucketname, response):
    """
    Finds the most recent updates and creation times for table rows
    to identify which values need to be updated
    """
    updates = []
    creations = []
    table = get_parquet(title[0], bucketname, response)
    for date in set(table['last_updated']):
        bisect.insort(updates, date)
    for date in set(table['created_at']):
        bisect.insort(creations, date)

    # Stores the most recent values from our previous readings
    last_update = updates[len(updates)-1]
    last_creation = creations[len(creations)-1]

    # Compile a sorted list of pre-existing 'last_updated' values
    # and another of 'created_at' values
    # returns most recent values in dict
    return {
        'created_at': last_creation,
        'last_updated': last_update
    }


def check_each_table(tables, dbcur, bucketname):
    """
    Gets the newly added data and pushes to a dict in parquet format
    """
    to_be_added = []
    response = get_file_info_in_bucket(bucketname)

    for title in tables:
        # if there are no existing parquet files storing our data, create them
        if not check_table_in_bucket(title, response):
            print(title[0], "to be added")
            rows, keys = get_whole_table(dbcur, title)
            to_be_added.append({title[0]: pd.DataFrame(rows, columns=keys)})
        else:
            # extract the most recent readings
            most_recent_readings = get_most_recent_time(
                title, bucketname, response)
            # extract raw data
            readings_created_at = most_recent_readings['created_at']
            readings_updated = most_recent_readings['last_updated']
            rows, keys = get_recents_table(
                dbcur, title, readings_created_at, readings_updated)
            results = [dict(zip(keys, row)) for row in rows]

            # if there any readings, add them to a dict
            # with the table title as a key
            # append them into the to_be_added list
            # pd.DataFrame will transform the data into a pandas parquet format
            if len(results) > 0:
                print(title[0], " is newer")
            else:
                print(title[0], "is not new")

            # if len(new_rows) > 0:print({title[0]: pd.DataFrame(new_rows)})
            if len(results) > 0:
                to_be_added.append({title[0]: pd.DataFrame(results)})
    return to_be_added


def push_to_cloud(local_object, bucketname):
    """
    Subfunction that pushes local_object to the cloud
    """
    # seperate key and value from object
    key = [key for key in local_object.keys()][0]
    values = local_object[key]

    # use key for file name, and value as the content for the file
    values.to_parquet(f'/tmp/{key}.parquet')

    s3_client = boto3.client('s3')
    s3_client.upload_file(f'/tmp/{key}.parquet', bucketname, f'{key}.parquet')
    os.remove(f'/tmp/{key}.parquet')

    return True


def add_updates(updates, bucketname):
    """
    Iterates through the list of dicts that need to be updated
    and push to the cloud
    """
    for local_object in updates:
        push_to_cloud(local_object, bucketname)


def index(dotenv_path_string):
    """
    Integrates all subfunctions to connect to AWS RDS,
    find a list of table names, iterate through them
    to evaluate whether there any updates to make.

    if so, return a list of all neccessary updates
    in pandas parquet format,
    if not exit the programme.
    """
    # connect to AWS RDS
    conn = make_connection(dotenv_path_string)
    dbcur = conn.cursor()

    # get bucket name
    bucketname = get_bucket_name('scrumptious-squad-in-data-')

    # Executes SQL query for finding a list of table names inside RDS
    # and store it in tables variable
    tables = get_titles(dbcur)

    # Iterates through the table_names and checks for
    #  any values which need to updated,
    # storing them in the 'updates' variable.
    updates = check_each_table(tables, dbcur, bucketname)
    dbcur.close()

    add_updates(updates, bucketname)


# Lambda handler
def extract_lambda_handler(event, context=None):
    """
    Fully integrated all subfunctions
    """
    index(event['dotenv_path_string'])
    logger.info("Completed")
    print("done")
    print(context)
