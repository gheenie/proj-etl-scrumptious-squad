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

def test_dim_date():
    start_date = '2023-03-26'
    end_date = '2023-03-27'
    dim_date = create_dim_date(start_date, end_date)
    assert dim_date.shape[1] == 8
    assert dim_date['year'][1] == 2023
    assert dim_date['month'][1] == 3
    assert dim_date['day'][1] == 27
    assert dim_date['day_of_week'][1] == 1
    assert dim_date['day_name'][1] == 'Monday'
    assert dim_date['month_name'][1] == 'March'
    assert dim_date['quarter'][1] == 1


def test_dim_location():
    dim_location = create_dim_location()
    assert dim_location.shape[1] == 8

def test_dim_design():
    dim_design = create_dim_design()
    assert dim_design.shape[1] == 4

def test_dim_currency():
    dim_currency = create_dim_currency()
    assert dim_currency.shape[1] == 3

def test_dim_counterparty():
    dim_counterparty = create_dim_counterparty()
    assert dim_counterparty.shape[1] == 9

def test_dim_staff():
    dim_staff = create_dim_staff()
    assert dim_staff.shape[1] == 6


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
