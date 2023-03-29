import pandas as pd


expected_output = pd.DataFrame({'hello' : ['world']
})

parquet = expected_output.to_parquet('load_test_db/hello_test.parquet')

