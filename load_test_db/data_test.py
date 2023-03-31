import pandas as pd


dim_date = pd.DataFrame({
    'date_id': pd.to_datetime(['2023-03-26', '2023-03-27']),
    'year': [2023, 2023],
    'month': [3, 3],
    'day': [26, 27],
    'day_of_week': [7, 1],
    'day_name': ['Sunday', 'Monday'],
    'month_name': ['March', 'March'],
    'quarter': [1, 1]
})

hello_df = pd.DataFrame({'hello' : ['world']})
dim_currency = pd.DataFrame({
    'currency_id': [0, 1],
    'currency_code': ['GBP', 'USD'],
    'currency_name': ['Pound', 'Dollar']
})

hello_df.to_parquet('load_test_db/hello_test.parquet')
dim_date.to_parquet('load_test_db/dim_date.parquet')
dim_currency.to_parquet('load_test_db/dim_currency.parquet')
