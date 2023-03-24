"""
To form your assertion statements, you need to refer to the
seeded data in extraction-test-db/setup-test-db.txt

Possible improvements: perform the Pandas functions with
pyarrow instead to make use of Parquet's full capabilities.
"""


from src.make_parquet import (index)
import pandas as pd


def test_example():
    EXTRACTION_SEED_FOLDER = 'database_access/data/parquet'
    SALES_ORDER_FILE = 'sales_order.parquet'

    # Extract test database into .parquet files.
    index('config/.env.test')
    # Read one table into a DataFrame.
    sales_order_table = pd.read_parquet(f'{EXTRACTION_SEED_FOLDER}/{SALES_ORDER_FILE}')
    
    assert sales_order_table.loc[sales_order_table.sales_order_id == 5][['staff_id']].values[0] == 2
