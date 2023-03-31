import boto3
import json


def create_secret(secret, secret_identifier):
    try:
        print("here")
        secrets_manager = boto3.client('secretsmanager')
        response = secrets_manager.create_secret(
            Name=secret_identifier,
            SecretString=json.dumps(secret)
        )
        #if secret is created send success message
        print('done')
        return "Success"
    except secrets_manager.exceptions.ResourceExistsException:
        #if name is taken return message
        return "Failure! Secret is taken"
    except Exception as e:
        #other error return this
        return {"UNKNOWN ERROR": e}




def get_inputs():
    # Get inputs from user
    secret_identifier = 'source_DB'

    database='totesys'
    user='project_user_4'
    password='LC7zJxE3BfvY7p'
    host='nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    port=5432 

    # Define the secret to be stored
    secret = {
        'user': user,
        'password': password,
        'database': database,
        'host':host,
        'port':5432
    }
    
    return secret, secret_identifier


def entry():

    secret, secret_id = get_inputs()    

    return create_secret(secret, secret_id)  

# print(entry())

def secrets_warehouse():

    secret_identifier = 'cred_DW'
    host = 'nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    port =  5432
    user = 'project_team_4'
    password = '9Fy8qy8VsqTjuN'
    database =  'postgres'
    schema =  'project_team_4'

    secret = {
        'user': user,
        'password': password,
        'database': database,
        'host':host,
        'port':port,
        'schema':schema

    }

    return secret, secret_identifier

# def entry():
#     secret, secret_id = secrets_warehouse()   
#     return create_secret(secret, secret_id)  

# print(entry())




