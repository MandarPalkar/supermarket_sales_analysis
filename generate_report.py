import sqlite3
import pandas as pd

# Establish SQLite connection
conn = sqlite3.connect('db/sales.db')
cursor = conn.cursor()

# SQL Quuery to find top 3 selling product lines in each branch
export_df = pd.read_sql("""
    WITH CTE AS(
        SELECT
            b.branch,
            p.product_line,
            ROUND(SUM(s.sales), 2) AS total_sales,
            DENSE_RANK() OVER (PARTITION BY b.branch ORDER BY ROUND(SUM(s.sales), 2) DESC) AS rank
        FROM sales_fact s
        JOIN branch_dim b ON b.branch_id = s.branch_id
        JOIN product_dim p ON p.product_id = s.product_id
        GROUP BY b.branch, p.product_line
    )
    SELECT 
        branch,
        product_line,
        total_sales
    FROM CTE
    WHERE rank <= 3
    ORDER BY branch, rank DESC
""", conn
)

# Export dataframe to CSV file
export_df.to_csv("data/report.csv", index=False)

# Commit changes and close connection
conn.commit()
conn.close()
