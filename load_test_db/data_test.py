import pandas as pd


# expected_output = pd.DataFrame({
#     'date_id': pd.to_datetime(['2023-03-26', '2023-03-27']),
#     'year': [2023, 2023],
#     'month': [3, 3],
#     'day': [26, 27],
#     'day_of_week': [7, 1],
#     'day_name': ['Sunday', 'Monday'],
#     'month_name': ['March', 'March'],
#     'quarter': [1, 1]
# })

# parquet = expected_output.to_parquet('load_test_db/load.parquet')


expected_output = pd.DataFrame({
    'currency_id': [0, 1],
    'currency_code': ['GBP', 'USD'],
    'currency_name': ['Pound', 'Dollar']
})


parquet = expected_output.to_parquet('load_test_db/dim_currency.parquet')

