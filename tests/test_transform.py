from src.transform import *
import pandas as pd

# one data frame = one table
# fact_sales_order
# dim_date
# dim_staff
# dim_location
# dim_currency
# dim_design
# dim_counterparty

# rearrange the parquet files to the above facts and dim tables using pandas

# check no duplicates
# check not null
# check datatype and convert to the star schema datatype (perhaps to be done at a later date - check with tutors)

def test_fact_sales_order_table_has_the_right_number_of_columns():

    sales_order_table = create_facts_sales_order_table()
    actual_output = sales_order_table.shape[1]
    expected_output = 15

    assert expected_output == actual_output

def test_sales_record_id_column_is_int_and_it_is_increasing_by_1_each_row():
    
    sales_order_table = create_facts_sales_order_table()

    assert sales_order_table['sales_record_id'][0] == 1
    assert sales_order_table['sales_record_id'][1] == 2
    assert sales_order_table['sales_record_id'][100] == 101
    assert sales_order_table['sales_record_id'][147] == 148
    assert sales_order_table['sales_record_id'][927] == 928

