import sqlite3
import pandas as pd

# 1. Establish a connection to your Sakila SQLite database
# Replace 'sakila.db' with the actual path to your file if it's in a different folder
conn = sqlite3.connect('sqlite-sakila.db')

# 2. Function: rentals_month
def rentals_month(engine, month, year):
    """
    Retrieves rental data for a specific month and year.
    """
    # We use strftime to filter by month and year in SQLite
    query = f"""
    SELECT * FROM rental 
    WHERE strftime('%m', rental_date) = '{month:02d}' 
    AND strftime('%Y', rental_date) = '{year}'
    """
    df = pd.read_sql_query(query, engine)
    return df

# 3. Function: rental_count_month
def rental_count_month(df, month, year):
    """
    Groups by customer_id and counts rentals for the given period.
    """
    column_name = f"rentals_{month:02d}_{year}"
    
    # Count rentals per customer
    count_df = df.groupby('customer_id')[['rental_id']].count()
    
    # Rename the column as requested
    count_df.rename(columns={'rental_id': column_name}, inplace=True)
    
    return count_df

# 4. Function: compare_rentals
def compare_rentals(df1, df2):
    """
    Merges two monthly DataFrames and calculates the difference.
    """
    # Merge on customer_id (inner join ensures they are in BOTH months)
    combined_df = pd.merge(df1, df2, on='customer_id', how='inner')
    
    # Calculate the difference (Month 2 - Month 1)
    # We use .iloc to grab the columns by index in case names change
    combined_df['difference'] = combined_df.iloc[:, 1] - combined_df.iloc[:, 0]
    
    return combined_df

# --- EXECUTION FLOW ---

# Step A: Get May 2005 data
may_rentals = rentals_month(conn, 5, 2005)
may_counts = rental_count_month(may_rentals, 5, 2005)

# Step B: Get June 2005 data
june_rentals = rentals_month(conn, 6, 2005)
june_counts = rental_count_month(june_rentals, 6, 2005)

# Step C: Compare them
comparison = compare_rentals(may_counts, june_counts)

# Show the results
print("Comparison of Active Customers (May vs June):")
print(comparison.head())

# Close the connection when done
conn.close()