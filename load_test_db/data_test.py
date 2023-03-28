import pandas as pd

data = {'name': ['Thalia', 'Naomi', 'Melanie', 'Alex', 'Gee', 'Dinesh']}
df = pd.DataFrame(data)

df.to_parquet('load_test_db/load.parquet')
