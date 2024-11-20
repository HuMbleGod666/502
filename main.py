import os
import pandas as pd
import sqlite3
import re

# Define the original directories and new directories
original_dirs = ['cat_1', 'cat_2', 'cat_3', 'cat_4']
new_dirs = ['cat_11', 'cat_22', 'cat_33', 'cat_44']
base_path = 'data'
new_base_path = 'data_new'

# Create the new base directory
os.makedirs(new_base_path, exist_ok=True)

# Function to sanitize table names by replacing special characters
def sanitize_table_name(name):
    # Replace any non-alphanumeric characters with underscores
    return re.sub(r'[^0-9a-zA-Z_]', '_', name)

# Iterate over the original directories and process files
for i, orig_dir in enumerate(original_dirs):
    # Create the corresponding new directory
    new_dir_path = os.path.join(new_base_path, new_dirs[i])
    os.makedirs(new_dir_path, exist_ok=True)
    
    # Create SQLite database for each category
    db_path = os.path.join(new_dir_path, f"{new_dirs[i]}.db")
    conn = sqlite3.connect(db_path)
    
    # List CSV files in the original directory
    orig_dir_path = os.path.join(base_path, orig_dir)
    csv_files = [f for f in os.listdir(orig_dir_path) if f.endswith('.csv')]
    
    for csv_file in csv_files:
        # Load each CSV file into a DataFrame
        csv_path = os.path.join(orig_dir_path, csv_file)
        df = pd.read_csv(csv_path)
        
        # Clean the table name by removing special characters
        table_name = sanitize_table_name(os.path.splitext(csv_file)[0])
        
        # Save the DataFrame to the SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    # Close the connection
    conn.close()

print("Data transfer to SQL databases completed successfully.")
