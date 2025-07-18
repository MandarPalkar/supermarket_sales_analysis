import kaggle
import sqlite3
import pandas as pd

# Download the dataset
dataset_name = 'faresashraf1001/supermarket-sales'
download_path = 'C:/Users/mandar/OneDrive/Desktop/66_degrees_assignment/'
kaggle.api.dataset_download_files(dataset_name, path=download_path, unzip=True)

# Establish SQLite connection
conn = sqlite3.connect('db/sales.db')
cursor = conn.cursor()

# Load CSV file into Pandas dataframe
df = pd.read_csv("C:/Users/mandar/OneDrive/Desktop/66_degrees_assignment/data/SuperMarket Analysis.csv")

# 1. Transforming raw CSV data 
# Rename columns (lowercase and replace spaces with underscores)
df.columns = df.columns.str.lower().str.replace(' ', '_')
df.rename(columns={'tax_5%': 'tax_amount', 'payment': 'payment_mode'}, inplace=True)

# Convert `date` & `time` columns into a single `invoice_datetime` column
df['invoice_datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'], format='%m/%d/%Y %I:%M:%S %p')

# 2. Transforming the Branch Data (branch, city combinations)
branch_df = df[['branch', 'city']].drop_duplicates()

# Generate branch_id column (Surrogate key)
branch_df['branch_id'] = range(1, len(branch_df) + 1) 
branch_df = branch_df[['branch_id', 'branch', 'city']]

# 3. Transforming the Customer Data (customer_type, gender combinations)
customer_df = df[['customer_type', 'gender']].drop_duplicates()

# Generate customer_id column (Surrogate key)
customer_df['customer_id'] = range(1, len(customer_df) + 1) 
customer_df = customer_df[['customer_id', 'customer_type', 'gender']]

# 4. Transforming the Product Data (product_line, unit_price, cogs combinations)
product_df = df[['product_line', 'unit_price']].drop_duplicates()
product_df['product_id'] = range(1, len(product_df) + 1)
product_df = product_df[['product_id', 'product_line', 'unit_price']]

# Generate product_id column (Surrogate key)
product_df = product_df[['product_id', 'product_line', 'unit_price']]

# 5. Insert data into the Branch Dimension
branch_df.to_sql('branch_dim', conn, if_exists='replace', index=False)

# 6. Insert data into the Customer Dimension
customer_df.to_sql('customer_dim', conn, if_exists='replace', index=False)

# 7. Insert data into the Product Dimension
product_df.to_sql('product_dim', conn, if_exists='replace', index=False)

# 8. Mapping customer, product, and branch ids from the dimensions into the sales data
df = df.merge(branch_df, on=['branch','city'], how='left')
df = df.merge(customer_df, on=['customer_type', 'gender'], how='left')
df = df.merge(product_df, on=['product_line', 'unit_price'], how='left')

# 9. Selecting required columns for the Sales Fact Table
sales_data = df[['invoice_id', 'branch_id', 'customer_id', 'product_id', 'quantity', 'cogs', 'tax_amount', \
                 'sales', 'gross_income', 'gross_margin_percentage', 'invoice_datetime', 'payment_mode', 'rating']]

# 10. Insert data into the Sales Fact Table
sales_data.to_sql('sales_fact', conn, if_exists='replace', index=False)

# Commit changes and close connection
conn.commit()
conn.close()
