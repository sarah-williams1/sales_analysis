import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from dask import dataframe as dd

df = pd.read_csv(r'E:\Northern Virginia Community College\ITD 245\ETL Project\online_sales.csv')
dask_ddf = dd.from_pandas(df, npartitions=2)
dask_ddf.compute()

print(df.head())

print(df.isnull().sum()) # checking for null values
print(df.describe())

# Standardizing the Dates
df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
# Dropping duplicate rows
df.drop_duplicates(inplace=True)
# Standardizing the columns
df['website_source'] = df['website_source'].str.lower().str.strip()


dask_ddf = dask_ddf.dropna(subset=['shipping_address'])
dask_ddf = dask_ddf.drop_duplicates()
dask_ddf = dask_ddf.compute()  # Materialize into Pandas after processing