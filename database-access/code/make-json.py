from pip._internal import main
import pg8000
import json
 
def connection():
     
    sql = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type= 'BASE TABLE';"""
     
    conn = pg8000.connect(
        database='totesys',
        user='******',
        password='*****',
        host='******',
        port=5432        
        )
         
    dbcur = conn.cursor()
    dbcur.execute(sql)
    tables = dbcur.fetchall()   

    for title in tables:       
        sql = f'SELECT * FROM {title[0]}'       
        dbcur.execute(sql)
        rows = dbcur.fetchall()     
        keys = [k[0] for k in dbcur.description]      
        results = [dict(zip(keys, row)) for row in rows]               
        with open(f'./data/JSON/{title[0]}.json', 'w') as f:
            f.write(json.dumps(results, indent=4, sort_keys=True, default=str))

    dbcur.close()   
     
    return True



try:
    connection()
    print("Complete")
except Exception as e:
    print(e)
    print("Failure")

