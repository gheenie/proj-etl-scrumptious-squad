"""
we will fill it in later
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


def pull_secrets():
    """
we will fill it in later
"""
    secret_name = 'source_DB'
    secrets_manager = boto3.client('secretsmanager')

    try:
        response = secrets_manager.get_secret_value(SecretId=secret_name)

    except ClientError as error:
        error_code = error.response['Error']['Code']

        print(error_code)
        if error_code == 'ResourceNotFoundException':
            raise Exception(f'ERROR: name not found') from error
        else:
            raise Exception(f'ERROR : {error_code}')
    else:
        secrets = json.loads(response['SecretString'])
        details = {
            'user': secrets['user'],
            'password': secrets['password'],
            'database': secrets['database'],
            'host': secrets['host'],
            'port': secrets['port']
        }
        print(details)
        return details['user'], details['password'], details['database'], details['host'], details['port']


def make_connection(dotenv_path_string):
    """
we will fill it in later
"""
    dotenv_path = Path(dotenv_path_string)
    load_dotenv(dotenv_path=dotenv_path)

    if dotenv_path_string.endswith('development'):
        user, password, database, host, port = pull_secrets()
        conn = pg8000.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port)
    elif dotenv_path_string.endswith('test'):
        conn = pg8000.connect(
            database=os.getenv('database'),
            user=os.getenv('user'),
            password=os.getenv('password')
        )
    return conn


def get_titles(dbcur):
    """
we will fill it in later
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
we will fill it in later
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
we will fill it in later
"""
    first_condition = f"(created_at > '{created}'::timestamp)"
    second_condition = f"(last_updated > '{updated}'::timestamp)"
    sql = f"SELECT * FROM {title[0]} WHERE ({first_condition}) OR ({second_condition})"
    try:
        dbcur.execute(sql)
        rows = dbcur.fetchall()
        keys = [k[0] for k in dbcur.description]
        return rows, keys
    except Exception as error:
        raise Exception(f"ERROR FETCHING TABLE {title[0]}: {error}") from error


def get_file_info_in_bucket(bucketname):
    """
we will fill it in later
"""
    try:
        s3_client = boto3.client('s3')
        return s3_client.list_objects_v2(Bucket=bucketname)
    except Exception as error:
        raise Exception(
            f"ERROR CHECKING OBJECTS IN BUCKET: {error}") from error


def get_bucket_name(bucket_prefix):
    """
we will fill it in later
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
we will fill it in later
"""
    if response['KeyCount'] == 0:
        return False
    filename = f"{title[0]}.parquet"
    filenames = [file['Key'] for file in response['Contents']]
    return filename in filenames


def get_parquet(title, bucketname, response):
    """
we will fill it in later
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
    function to find most recent update and creation times for table rows
    to check which values need to be updated
    https://www.striim.com/blog/change-data-capture-cdc-what-it-is-and-how-it-works/ 

    table = pd.read_parquet(f"./database-access/data/parquet/{title[0]}.parquet", engine='pyarrow')

    compile a sorted list of 'last_updated' values and another sorted list of 'created_at' values 
    existing inside previous readings

"""
    updates = []
    creations = []
    table = get_parquet(title[0], bucketname, response)
    for date in set(table['last_updated']):
        bisect.insort(updates, date)
    for date in set(table['created_at']):
        bisect.insort(creations, date)

    # stores the most recent values from our previous readings
    last_update = updates[len(updates)-1]
    last_creation = creations[len(creations)-1]

    # returns most recent values in dict
    return {
        'created_at': last_creation,
        'last_updated': last_update
    }


def check_each_table(tables, dbcur, bucketname):
    """
we will fill it in later
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

            # if there any readings, add them to a dict with the table title as a key.
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
we will fill it in later
"""
    # seperate key and value from object
    key = [key for key in local_object.keys()][0]
    values = local_object[key]

    # use key for file name, and value as the content for the file
    values.to_parquet(f'/tmp/{key}.parquet')

    # print(key)

    s3_client = boto3.client('s3')
    s3_client.upload_file(f'/tmp/{key}.parquet', bucketname, f'{key}.parquet')
    os.remove(f'/tmp/{key}.parquet')

    return True


def add_updates(updates, bucketname):
    """
we will fill it in later
"""
    # iterate through the list of dicts that need to be updated
    for local_object in updates:
        push_to_cloud(local_object, bucketname)


def index(dotenv_path_string):
    """
    we will fill it in later
    function to connect to AWS RDS, find a list of table names, iterate through them 
    and evaluate whether there any updates to make.
    if so, return a list of all neccessary updates in pandas parquet format
    if not exit the programme.

    iterate through the table_names and check for any values which need to updated, 
    storing them in the 'updates' variable

    execute SQL query for finding a list of table names inside RDS 
    and store it inside tables variable
"""
    # connect to AWS RDS
    conn = make_connection(dotenv_path_string)
    dbcur = conn.cursor()

    # get bucket name
    bucketname = get_bucket_name('scrumptious-squad-in-data-')

    tables = get_titles(dbcur)

    updates = check_each_table(tables, dbcur, bucketname)
    dbcur.close()

    add_updates(updates, bucketname)

# Lambda handler


def someting(event, context):
    index('config/.env.development')
    logger.info("Completed")
    print("done")
