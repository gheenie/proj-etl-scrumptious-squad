from src.transform import *
import pandas as pd

# check if we are using secret manager, if so we need to retrive the secret key

# read the parquet files from the s3 ingested data bucket

# Single function (Check_data_files)
# check the data is in the right format (readable parquet file) 
#       if corrupted or in wrong format dont execute 

# one data frame = one table
# fact_sales_order
# dim_date
# dim_staff
# dim_location
# dim_currency
# dim_design
# dim_counterparty

# rearrange the parquet files to the above facts and dim tables usign pandas

# read the parquet file into a daframe 

# df_address = pd.read_csv()
# 

# def create_facts_table():

# df_fact_sales_order = pd.Dataframe()

# check no duplicates
# check not null
# check datatype and convert to the star schema datatype (perhaps to be done at a later date - check with tutors)



def test_fact_sales_order_table_has_the_right_number_of_columns():

    sales_order_table = create_facts_sales_order_table()
    actual_output = sales_order_table.shape[1]
    expected_output = 15

    assert expected_output == actual_output

# def test_facts_sales_order_contains_the_latest_date_in_last_updated_column():

#     # test that the date on the last row of the sales_order table is yesterdays dtae

def test_sales_record_id_column_is_int_and_it_is_increasing_by_1_each_row():
    
    sales_order_table = create_facts_sales_order_table()
    actual_output = sales_order_table['sales_record_id'][3]
    expected_output = 4

    assert expected_output == actual_output