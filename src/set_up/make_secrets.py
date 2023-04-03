import boto3
import json
import os
from dotenv import load_dotenv
from pathlib import Path


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

    dotenv_path = Path('config/.env.development')
    load_dotenv(dotenv_path=dotenv_path)

    database = os.getenv('database')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')

    # Define the secret to be stored
    secret = {
        'user': user,
        'password': password,
        'database': database,
        'host': host,
        'port': port
    }
    
    return secret, secret_identifier


def entry():

    secret, secret_id = get_inputs()    

    return create_secret(secret, secret_id)  


# print(entry())


def secrets_warehouse():

    secret_identifier = 'cred_DW'

    dotenv_path = Path('config/.env.warehouse')
    load_dotenv(dotenv_path=dotenv_path)

    database = os.getenv('database')
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    schema = os.getenv('schema')

    secret = {
        'user': user,
        'password': password,
        'database': database,
        'host':host,
        'port':port,
        'schema':schema

    }

    return secret, secret_identifier


def entry_warehouse():
    secret, secret_id = secrets_warehouse()   
    return create_secret(secret, secret_id)  


# print(entry_warehouse())





