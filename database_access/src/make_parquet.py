from pip._internal import main
import pg8000
import pandas as pd
import pyarrow as pq
from os.path import isfile
from datetime import datetime
import bisect
import sys
import boto3
import json
from botocore.exceptions import ClientError
import psycopg


def pull_secrets():
    #get secret_id for secret
    secret_name = input('Get: Secret Identifier:')     
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
        with open('secrets.json', 'w') as f:
            
            f.write(response['SecretString'])
        
            print(f"{response['Name']} saved in local file 'secrets.json'")
                 

        #return results
        return json.loads(response['SecretString'])
    

def get_titles(dbcur):
    sql = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type= 'BASE TABLE';"""
    dbcur.execute(sql)
    return dbcur.fetchall()  

     
     
def check_table_exists(title):    
        if isfile(f"./database-access/data/parquet/{title[0]}.parquet"): 
            return True
        else:
            return False  
        

def get_table(dbcur, title):
    sql = f'SELECT * FROM {title[0]}'  
    print(title)
    dbcur.execute(sql)
    rows = dbcur.fetchall()     
    keys = [k[0] for k in dbcur.description] 
    #print([dict(zip(keys, row)) for row in rows])
    return rows, keys




def make_connection():
    conn = pg8000.connect(
    database='totesys',
    user='project_user_4',
    password='LC7zJxE3BfvY7p',
    host='nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com',
    port=5432        
    )

    return conn      

     
     

def get_most_recent_time(title):
    #function to find most recent update and creation times for table rows to check which values need to be updated
    #https://www.striim.com/blog/change-data-capture-cdc-what-it-is-and-how-it-works/
    updates = []
    creations = []
    
    #read existing values
    table = pd.read_parquet(f"./database-access/data/parquet/{title[0]}.parquet", engine='pyarrow')


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
        #if there are no existing parquet files storing our data, create them
        if not check_table_exists(title):                                      
            rows, keys = get_table(dbcur, title)
            to_be_added.append({title[0]: pd.DataFrame(rows, columns=keys)})
        else:
            #extract the most recent readings
            most_recent_readings = get_most_recent_time(title)

            #extract raw data
            rows, keys = get_table(dbcur, title)
            results = [dict(zip(keys, row)) for row in rows]

            #filter data to find readings with a more recent 'creation time' or 'update time' than our most recent readings have             
            new_rows = [row for row in results if row['created_at'] > most_recent_readings['created_at'] or row['last_updated'] > most_recent_readings['last_updated']]                              
            
            #if there any readings, add them to a dict with the table title as a key.             
            #append them into the to_be_added list
            #pd.DataFrame will transform the data into a pandas parquet format
            #if there are no updates to make, exit the programme.
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
        values.to_parquet(f"./database-access/data/parquet/{key}.parquet") 
     
        return True


def  add_updates(updates):
    #iterate through the list of dicts that need to be updated     
    for object in updates:                 
         push_to_cloud(object)

 


 

def index(): 

    #function to connect to AWS RDS, find a list of table names, iterate through them and evaluate whether there any updates to make.
    #if not exit the programme.
    #if so, return a list of all neccessary updates in pandas parquet format
     
     #connect to AWS RDS 
    conn = make_connection()        
    dbcur = conn.cursor()
    

    #execute SQL query for finding a list of table names inside RDS and store it inside tables variable
    tables = get_titles(dbcur)
    print(tables)


    #iterate through the table_names and check for any values which need to updated, storing them in the 'updates' variable
    updates = check_each_table(tables, dbcur)                 
                                  
                 
    #close connection
    dbcur.close() 
    return 1 

    add_updates(updates)



index()




