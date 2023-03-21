from pip._internal import main
import pg8000
import csv
 
def connection():
     
    sql = """SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type= 'BASE TABLE';"""
     
    conn = pg8000.connect(
        database='totesys',
        user='project_user_4',
        password='LC7zJxE3BfvY7p',
        host='nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com',
        port=5432        
        )
         
    dbcur = conn.cursor()
    dbcur.execute(sql)
    tables = dbcur.fetchall()


    for title in tables:
        sql = f"select * from {title[0]}"           
        dbcur.execute(sql)    
        rows = dbcur.fetchall() 
        header = [k[0] for k in dbcur.description]  
        with open(f'./data/csv/{title[0]}.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in rows:
                writer.writerow(row)
            


    dbcur.close()   
     
    return True



try:
    connection()
    print("Complete")
except Exception as e:
    print(e)
    print("Failure")