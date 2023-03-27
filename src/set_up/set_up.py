from make_secrets import entry as make_secret
from make_bucket import make_bucket

def index(): 

    bucket_name = make_bucket()
    print(bucket_name)
    
    make_secret()