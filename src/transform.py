import pandas as pd

df_address = pd.read_csv('./database_access/data/csv/address.csv')



def create_facts_sales_order_table():
    column_name_list = ["sales_record_id","sales_order_id", "created_date", "created_time", "last_updated_date", "last_updated_time", "sales_staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "design_id", "agreed_payment_date", "agreed_delivery_date", "agreed_delivery_location_id"]
    
    sales_order_table = pd.DataFrame(columns = column_name_list) 
    
    return sales_order_table