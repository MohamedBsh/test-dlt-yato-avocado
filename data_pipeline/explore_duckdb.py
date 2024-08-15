import duckdb
import pandas as pd

def safe_execute(con, query):
    try:
        return con.execute(query).fetchall()
    except Exception as e:
        print(f"Error executing query '{query}': {e}")
        return None

def explore_avocats_data(db_path):
    try:
        # Connect to the database
        con = duckdb.connect(db_path, read_only=True)
        print(f"Successfully connected to database: {db_path}")

        # List all schemas
        schemas = safe_execute(con, "SELECT schema_name FROM information_schema.schemata")
        if schemas:
            print("Schemas in the database:", [schema[0] for schema in schemas])

        # Check if 'avocats_data' table exists in the 'avocats' schema
        table_check = safe_execute(con, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'avocats' AND table_name = 'avocats_data'")
        
        if table_check:
            print("\nTable 'avocats.avocats_data' found. Fetching information...")
            
            # Fetch column names
            columns = safe_execute(con, "SELECT column_name FROM information_schema.columns WHERE table_schema = 'avocats' AND table_name = 'avocats_data'")
            if columns:
                print("Columns in avocats.avocats_data:", [col[0] for col in columns])

            # Get row count
            row_count = safe_execute(con, "SELECT COUNT(*) FROM avocats.avocats_data")
            if row_count:
                print(f"\nTotal number of rows in avocats.avocats_data: {row_count[0][0]}")

            # Preview data (first column only)
            if columns:
                first_column = columns[0][0]
                preview_query = f"SELECT {first_column} FROM avocats.avocats_data LIMIT 5"
                preview_data = safe_execute(con, preview_query)
                if preview_data:
                    print(f"\nPreview of first column ({first_column}):")
                    for row in preview_data:
                        print(row[0])

            # Display unique values count in the first column
            if columns:
                first_column = columns[0][0]
                unique_count_query = f"SELECT COUNT(DISTINCT {first_column}) FROM avocats.avocats_data"
                unique_count = safe_execute(con, unique_count_query)
                if unique_count:
                    print(f"\nNumber of unique values in '{first_column}': {unique_count[0][0]}")
        else:
            print("\nTable 'avocats.avocats_data' not found in the database.")

        con.close()

    except duckdb.Error as e:
        print(f"An error occurred while exploring the database: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    db_path = 'avocats_evolution.duckdb'
    explore_avocats_data(db_path)