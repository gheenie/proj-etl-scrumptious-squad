import boto3
import pandas as pd
from io import BytesIO

def get_parquet(title):
    bucketname = 'nicebucket1679649834'  
    s3 = boto3.client('s3')
    files =s3.list_objects_v2(Bucket=bucketname)
    filename = f"{title}.parquet"      
    if filename in [file['Key'] for file in files['Contents']]:       
        print(filename)    
        buffer = BytesIO()
        client = boto3.resource('s3')
        object=client.Object(bucketname, filename)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer)
        return df


def check_table_in_bucket(title):    
        bucketname = 'nicebucket1679649834'  
        s3 = boto3.client('s3')
        files =s3.list_objects_v2(Bucket=bucketname)
        filename = f"{title[0]}.parquet"  
        filenames= [file['Key'] for file in files['Contents']]
        
        return filename in filenames
        
            
   

