"""
To form your assertion statements, you need to refer to the
seeded data in extraction-test-db/setup-test-db.txt

Possible improvements: perform the Pandas functions with
pyarrow instead to make use of Parquet's full capabilities.
"""


from database_access.src.make_parquet import (index)
import pandas as pd
import pyarrow.parquet as pq


def test_example():
    EXTRACTION_SEED_FOLDER = 'database_access/data/parquet'
    SALES_ORDER_FILE = 'sales_order.parquet'

    # Extract test database into .parquet files.
    index('config/.env.test')
    # Read one table into a DataFrame.
    sales_order_table = pd.read_parquet(f'{EXTRACTION_SEED_FOLDER}/{SALES_ORDER_FILE}')

    test_pq = pq.read_table(f'{EXTRACTION_SEED_FOLDER}/{SALES_ORDER_FILE}')
    print(test_pq)
    
    # Example statements to demonstrate Pandas functions. Refer to NC Notes as well.
    # The first square brackets is similar to a WHERE condition in sql.
    print(sales_order_table.loc[sales_order_table.sales_order_id == 1416])
    # The second square brackets is to filter out unnecessary columns. Note that they're
    # nested, unnested gives different results.
    print(sales_order_table.loc[sales_order_table.sales_order_id == 1416][['staff_id']])
    # The previous result is still in a 'Series'. You need to access the cell with .values
    # to turn it into an int that you can assert with. Currently address_table is one column
    # because of previous filter, [0] because there's only one row.
    print(sales_order_table.loc[sales_order_table.sales_order_id == 1416][['staff_id']].values[0])
    print(sales_order_table.loc[sales_order_table.sales_order_id == 1416][['staff_id']].values[0] == 3)
    
    # This is a fake test to show that the assert works cause actual testing is currently failing.
    assert sales_order_table.loc[sales_order_table.sales_order_id == 1416][['staff_id']].values[0] == 3
    # This is the actual test. The fail message is because there isn't a row with sales_order_id == 5.
    assert sales_order_table.loc[sales_order_table.sales_order_id == 5][['staff_id']].values[0] == 2
