import os
import sqlite3
import pandas as pd

def query_database():
    base_path = 'data_new'  # Fixed base path

    # Ask the user whether they want to search in one folder or all folders
    search_scope = input("Do you want to search in a single folder or in all available folders? (Enter 'one' or 'all'): ").strip().lower()

    # List of subfolders to search within 'data_new'
    all_folders = ['cat_11', 'cat_22', 'cat_33', 'cat_44']

    # Define columns to keep for each folder
    folder_columns = {
        'cat_11': ['Initiative Sponsor','Request ID','Request Name','Request Type', 'Department', 'Domain','Area','Function','Cost Center ID','Cost Center Name','Headcount','Amount'],
        'cat_22': ['Initiative Sponsor','Request ID','Request Name','YTD Budget (Headcount)','YTD Actuals (Headcount)','YTD Variance (Headcount)','YTD Budget ($)','YTD Actuals ($)','YTD Variance ($)','FY Budget (Headcount)','FY Fcst (Headcount)','FY Variance (Headcount)','FY Budget ($)','FY Fcst ($)','FY Variance ($)'],  
        'cat_33': ['Initiative Sponsor','Request ID','Request Name','Request Type', 'Department', 'Domain','Area','Function','Cost Center ID','Cost Center Name','Headcount','Amount'],
        'cat_44': ['Initiative Sponsor']
    }

    # Determine the scope of search
    if search_scope == 'one':
        selected_folder = input(f"Enter the folder to search in ({', '.join(all_folders)}): ").strip()

        if selected_folder not in all_folders:
            print("Invalid folder selection. Please check your input.")
            return

        folders_to_search = [selected_folder]
    elif search_scope == 'all':
        folders_to_search = all_folders
    else:
        print("Invalid input. Please enter 'one' or 'all'.")
        return

    found_rows = []

    # Iterate over each folder to search for the selected Request ID
    for folder in folders_to_search:
        folder_path = os.path.join(base_path, folder)
        
        if not os.path.exists(folder_path):
            print(f"The folder {folder_path} does not exist. Skipping...")
            continue

        db_files = [f for f in os.listdir(folder_path) if f.endswith('.db')]

        if not db_files:
            print(f"No database found in the folder {folder_path}.")
            continue

        db_path = os.path.join(folder_path, db_files[0])
        conn = sqlite3.connect(db_path)

        request_id = input("Enter the Initiative Sponsor you want to search for: ")

        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        for table_name in tables:
            table = table_name[0]
            try:
                query = f"SELECT * FROM {table} WHERE `Initiative Sponsor` = ?"
                df = pd.read_sql_query(query, conn, params=(request_id,))

                if not df.empty:
                    # Keep only the relevant columns if specified
                    if folder_columns[folder]:
                        columns_to_keep = folder_columns[folder]
                        available_columns = [col for col in columns_to_keep if col in df.columns]
                        df = df[available_columns]

                    found_rows.append((folder, table, df))
            except Exception as e:
                print(f"Error querying table {table} in {db_path}: {e}")

        conn.close()

    # Save and print the results
    if found_rows:
        output_file = f"query_results_{search_scope}_folders.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            for folder, table, df in found_rows:
                if not df.empty:
                    f.write(f"Results from {folder} ,{table}\n")
                    df.to_csv(f, index=False)
                    f.write("\n")  # Write an empty row for separation
        print(f"Results have been saved to {output_file}")
    else:
        print("No matching records found for the given Request ID.")

query_database()
