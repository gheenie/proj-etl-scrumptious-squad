import pg8000
import pandas as pd
import bisect
import boto3
import json
from botocore.exceptions import ClientError
from io import BytesIO
import os
from dotenv import load_dotenv
from pathlib import Path


def pull_secrets():
    
    secret_name = 'source_DB'     
    secrets_manager = boto3.client('secretsmanager')

    try:               
        response = secrets_manager.get_secret_value(SecretId=secret_name)  

    except ClientError as e:            
        error_code = e.response['Error']['Code']

        print(error_code)
        if error_code == 'ResourceNotFoundException':            
            return (f'ERROR: name not found') 
        else:           
            return(f'ERROR : {error_code}')
    else:
        secrets = json.loads(response['SecretString'])
        
        details = {
        'user': secrets['user'][0],
        'password': secrets['password'][0],
        'database': secrets['database'][0],
        'host':secrets['host'][0],
        'port':secrets['port']
        }
        
        return details['user'], details['password'], details['database'], details['host'], details['port'],


def make_connection(dotenv_path_string): 
    dotenv_path = Path(dotenv_path_string)

    load_dotenv(dotenv_path=dotenv_path)

    if dotenv_path_string.endswith('development'):
        user, password, database, host, port = pull_secrets()  
        conn = pg8000.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port        
        )        
    elif dotenv_path_string.endswith('test'):
        conn = pg8000.connect(
            database=os.getenv('database'),
            user=os.getenv('user'),
            password=os.getenv('password')
        )
    
    return conn
    

def get_titles(dbcur):
    sql = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type= 'BASE TABLE';"""
    dbcur.execute(sql)
    return dbcur.fetchall()  


def get_table(dbcur, title):
    sql = f'SELECT * FROM {title[0]}'    
    dbcur.execute(sql)
    rows = dbcur.fetchall()     
    keys = [k[0] for k in dbcur.description]   
    return rows, keys


def get_bucket_name(bucket_prefix):
    s3 = boto3.client('s3')
    response = s3.list_buckets()

    for bucket in response['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            return bucket['Name']


def check_table_in_bucket(title):    
        bucketname = get_bucket_name('scrumptious-squad-in-data-')
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucketname)

        if response['KeyCount'] == 0: return False

        filename = f"{title[0]}.parquet"  
        filenames= [file['Key'] for file in response['Contents']]
        
        return filename in filenames


def get_parquet(title):
    bucketname = get_bucket_name('scrumptious-squad-in-data-')
    s3 = boto3.client('s3')
    response =s3.list_objects_v2(Bucket=bucketname)
    filename = f"{title}.parquet"      

    if response['KeyCount'] == 0: return False

    if filename in [file['Key'] for file in response['Contents']]:       
        print(filename)    
        buffer = BytesIO()
        client = boto3.resource('s3')
        object=client.Object(bucketname, filename)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
        return df


def get_most_recent_time(title):
    #function to find most recent update and creation times for table rows to check which values need to be updated
    #https://www.striim.com/blog/change-data-capture-cdc-what-it-is-and-how-it-works/
    updates = []
    creations = []
    
    #read existing values

    #table = pd.read_parquet(f"./database-access/data/parquet/{title[0]}.parquet", engine='pyarrow')
    table = get_parquet(title[0]) 
       

    #compile a sorted list of 'last_updated' values and another sorted list of 'created_at' values existing inside previous readings
    for date in set(table['last_updated']):bisect.insort(updates, date)
    for date in set(table['created_at']):bisect.insort(creations, date)    

    #stores the most recent values from our previous readings
    last_update = updates[len(updates)-1]             
    last_creation = creations[len(creations)-1]

    #returns most recent values in dict
    return {        
        'created_at': last_update,
        'last_updated': last_creation
    }


def check_each_table(tables, dbcur):     
    to_be_added= []

    for title in tables:                      
        rows, keys = get_table(dbcur, title)
        
        #if there are no existing parquet files storing our data, create them
        if not check_table_in_bucket(title): 
            print(title, "to be added")          
            to_be_added.append({title[0]: pd.DataFrame(rows, columns=keys)})
        else:
            #extract the most recent readings
            most_recent_readings = get_most_recent_time(title)

            #extract raw data
            results = [dict(zip(keys, row)) for row in rows]

            #filter data to find readings with a more recent 'creation time' or 'update time' than our most recent readings have             
            new_rows = [row for row in results if row['created_at'] > most_recent_readings['created_at'] or row['last_updated'] > most_recent_readings['last_updated']]                              
            
            #if there any readings, add them to a dict with the table title as a key.             
            #append them into the to_be_added list
            #pd.DataFrame will transform the data into a pandas parquet format
            #if there are no updates to make, exit the programme.
            print(title)
            if len(new_rows) >0:print("is newer")
            else: print("is not new")
            
            #if len(new_rows) > 0:print({title[0]: pd.DataFrame(new_rows)})
            if len(new_rows) > 0:to_be_added.append({title[0]: pd.DataFrame(new_rows)})

    # for keyval in to_be_added:
    #     for value in keyval.values():
    #         print(value)     
   
    return to_be_added


def push_to_cloud(object): 
        #seperate key and value from object              
        key = [key for key in object.keys()][0]
        values = object[key] 

        #use key for file name, and value as the content for the file       
        values.to_parquet(f'./database_access/data/parquet/{key}.parquet') 

        print(key)

        s3 = boto3.client('s3')
        bucketname = get_bucket_name('scrumptious-squad-in-data-')

        out_buffer = BytesIO()
        # values.to_parquet(out_buffer, index=False, compression="gzip")

        s3.upload_file(f'./database_access/data/parquet/{key}.parquet', bucketname, f'{key}.parquet')
        os.remove(f'./database_access/data/parquet/{key}.parquet')        
       
     
        return True


def  add_updates(updates):
    #iterate through the list of dicts that need to be updated     
    for object in updates:                 
         push_to_cloud(object)
 


def index(dotenv_path_string): 

    #function to connect to AWS RDS, find a list of table names, iterate through them and evaluate whether there any updates to make.
    #if not exit the programme.
    #if so, return a list of all neccessary updates in pandas parquet format  
     
     #connect to AWS RDS 
    conn = make_connection(dotenv_path_string)        
    dbcur = conn.cursor()    

    #execute SQL query for finding a list of table names inside RDS and store it inside tables variable
    tables = get_titles(dbcur)
    

    #iterate through the table_names and check for any values which need to updated, storing them in the 'updates' variable
    updates = check_each_table(tables, dbcur)  
    dbcur.close()                
                                  
            
    add_updates(updates)



# index('config/.env.development')


