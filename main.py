import pandas as pd
import numpy as np
from dask import dataframe as dd
from sqlalchemy import create_engine

# Purpose: Conduct the ETL process on a CSV for customer data

# BEGIN EXTRACTION
# Load the CSV data into a pandas DataFrame
df = pd.read_csv(r'E:\Northern Virginia Community College\ITD 245\ETL Project\online_sales.csv')

# BEGIN TRANSFORMATION

pd.set_option('display.max_columns', None)
print("Initial Data Preview:\n", df.head(), '\n\n')
print("Number of Null Values:\n", df.isnull().sum(), '\n\n')  # Check for missing values

# Cleaning the data
df.drop_duplicates(inplace=True)  # Remove duplicate rows
df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')  # Standardize date format
for col in df.columns:
    if df[col].dtype == 'object':  # Apply cleaning to text-based columns
        df[col] = df[col].str.strip().str.lower()  # Strip whitespace and lowercase text

# Integrating data
df['customer_id'] = pd.factorize(df['customer_id'])[0]  # Create unified customer ID
# Example integration step (merge datasets if necessary)
# combined_df = pd.merge(df_online_sales, df_instore_sales, on='customer_id', how='inner')
# combined_df = pd.merge(combined_df, df_products, on='product_id', how='left')

# Partitioning the data using Dask for efficient processing
dask_ddf = dd.from_pandas(df, npartitions=2)

# Data Enrichment
# Calculate derived metrics
df['total_purchase_value'] = df['quantity'] * df['price']  # Total purchase value per order
avg_order_value = df.groupby('customer_id')['total_purchase_value'].mean().reset_index(name='avg_order_value')
# Example enrichment step (add external information)
# df = pd.merge(df, product_info, on='product_id', how='left')
# df = pd.merge(df, customer_info, on='customer_id', how='left')

# Data Aggregation
# Aggregating data at customer and product levels
customer_summary = df.groupby('customer_id').agg({
    'total_purchase_value': 'sum',
    'quantity': 'sum',
    'price': 'mean'
}).reset_index()

product_summary = df.groupby('product_id').agg({
    'quantity': 'sum',
    'total_purchase_value': 'sum',
    'price': 'mean'
}).reset_index()

# Denormalization
# Flatten data structure to create a single table for analysis
flattened_df = df.pivot_table(
    index='customer_id',
    columns='product_id',
    values=['quantity', 'total_purchase_value'],
    aggfunc='sum'
).reset_index()

flattened_df.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in flattened_df.columns]

# Cleaning data partitions using Dask
dask_ddf = dask_ddf.dropna(subset=['shipping_address'])
dask_ddf = dask_ddf.drop_duplicates()

# Convert Dask DataFrame back to pandas DataFrame
df = dask_ddf.compute()

# Align all columns to the right for display
df.style.set_properties(**{'text-align': 'right'})

# BEGIN LOAD
# Export the cleaned DataFrame to a new CSV file
df.to_csv(r'E:\Northern Virginia Community College\ITD 245\ETL Project\cleaned_online_sales.csv', index=False)

engine = create_engine('mysql+pymysql://root:MySQLRootPassword@localhost:3306/etl_final')
df.to_sql(name='online_sales_denormalized', con=engine, if_exists='replace', index=False)
