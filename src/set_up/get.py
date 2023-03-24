import json
import boto3
from botocore.exceptions import ClientError

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
        'port':5432
        }
        return details

        

print(pull_secrets())
