import pandas as pd
from dotenv import load_dotenv
import os
from decouple import config
import pg8000

# converts to human-readable format for error handling
def read_data(parquet_path):
    return pd.read_parquet(f"${parquet_path}", engine = "auto")

# Checks data is not corrupted
def corruption_checker():
    pass

# Make connection to data warehouse
def make_warehouse_connection():
    load_dotenv()
    API_HOST=config('host')
    API_USER=config('user')
    API_PASS=config('password')
    API_DBASE=config('database')
    conn = pg8000.connect(
        host = API_HOST,
        user = API_USER,
        password = API_PASS,
        database = API_DBASE
    )
    return conn
    
# Pushes parqueted data to data warehouse
def insert_into_warehouse():
    pass

# Integrated function to combine all of the above
def load_to_warehouse():
    try:
        # Log status message to confirm load
        pass
    except:
        # Log status with helpful error message
        pass

# Lambda_handler
def lambda_handler():
    pass