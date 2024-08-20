import duckdb

def safe_execute(con, query):
    try:
        return con.execute(query).fetchall()
    except Exception as e:
        print(f"Error executing query '{query}': {e}")
        return None

def explore_avocats_data(db_path):
    try:
        con = duckdb.connect(db_path, read_only=True)
        print(f"Successfully connected to database: {db_path}")

        schemas = safe_execute(con, "SELECT schema_name FROM information_schema.schemata")
        if schemas:
            print("Schemas in the database:", [schema[0] for schema in schemas])

        table_found = False
        for schema in schemas:
            schema_name = schema[0]
            tables = safe_execute(con, f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'")
            print(f"Tables in schema {schema_name}:", [table[0] for table in tables])
            
            if 'avocats_data' in [table[0] for table in tables]:
                table_found = True
                print(f"\nTable 'avocats_data' found in schema '{schema_name}'. Fetching information...")
                
                columns = safe_execute(con, f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = 'avocats_data'")
                if columns:
                    print(f"Columns in {schema_name}.avocats_data:", [col[0] for col in columns])

                row_count = safe_execute(con, f"SELECT COUNT(*) FROM {schema_name}.avocats_data")
                if row_count:
                    print(f"\nTotal number of rows in {schema_name}.avocats_data: {row_count[0][0]}")

                # Select all values from the 'name' column
                name_query = f"SELECT name FROM {schema_name}.avocats_data"
                name_data = safe_execute(con, name_query)
                if name_data:
                    print("\nAll values in the 'name' column:")
                    for row in name_data:
                        print(row[0])
                else:
                    print("No 'name' column found or it's empty.")

        if not table_found:
            print("\nTable 'avocats_data' not found in any schema of the database.")

        con.close()

    except duckdb.Error as e:
        print(f"An error occurred while exploring the database: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    db_path = 'avocats_evolution.duckdb'
    explore_avocats_data(db_path)