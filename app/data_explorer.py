import duckdb
import os
import shutil
import logging
import pandas as pd
from tabulate import tabulate
import requests
import io
from config import URL, DB_PATH, SCHEMA_NAME, PARQUET_DIR

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_db():
    """Establish a connection to the database."""
    return duckdb.connect(DB_PATH)

def clean_database():
    """Clean the database by dropping all tables, deleting Parquet files, and performing a deep clean."""
    with connect_to_db() as conn:
        try:
            tables = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{SCHEMA_NAME}'").fetchall()
            for table in tables:
                table_name = table[0]
                conn.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{table_name}")
                logging.info(f"Table '{SCHEMA_NAME}.{table_name}' successfully dropped.")
            logging.info(f"All tables in '{SCHEMA_NAME}' schema have been dropped.")
        except Exception as e:
            logging.error(f"Error while dropping tables: {str(e)}")

    delete_parquet_files()
    deep_clean()

def delete_parquet_files():
    """Delete Parquet files from the specified directory."""
    try:
        if os.path.exists(PARQUET_DIR):
            shutil.rmtree(PARQUET_DIR)
            logging.info("Parquet files successfully deleted.")
        else:
            logging.info("Parquet directory does not exist. Nothing to delete.")
    except Exception as e:
        logging.error(f"Error while deleting Parquet files: {str(e)}")

def deep_clean():
    """Perform a deep clean of all DLT related files and caches."""
    paths_to_clean = [
        os.path.expanduser("~/.dlt"),
        os.path.expanduser("~/.cache/dlt"),
        PARQUET_DIR,
        os.path.dirname(DB_PATH)
    ]
    
    for path in paths_to_clean:
        if os.path.exists(path):
            try:
                if os.path.isfile(path):
                    os.remove(path)
                else:
                    shutil.rmtree(path)
                logging.info(f"Cleaned: {path}")
            except Exception as e:
                logging.error(f"Error cleaning {path}: {str(e)}")

    # Nettoyage spécifique des fichiers de jobs DLT
    dlt_pipeline_dir = os.path.expanduser("~/.dlt/pipelines/avocats_evolution")
    if os.path.exists(dlt_pipeline_dir):
        for root, dirs, files in os.walk(dlt_pipeline_dir):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)
                        logging.info(f"Removed DLT job file: {file_path}")
                    except Exception as e:
                        logging.error(f"Error removing DLT job file {file_path}: {str(e)}")

    logging.info("Deep clean completed.")

def load_csv_data():
    """Load CSV data from the URL specified in the config."""
    response = requests.get(URL)
    df = pd.read_csv(io.StringIO(response.text))
    logging.info(f"Data loaded, shape: {df.shape}")
    logging.info(f"Columns: {df.columns.tolist()}")
    logging.info(f"Data types: {df.dtypes}")
    return df

def explore_csv_data(df):
    """Explore the loaded CSV data."""
    logging.info(f"Columns in CSV: {df.columns.tolist()}")
    logging.info(f"Number of columns in CSV: {len(df.columns)}")
    logging.info(f"Data types:\n{df.dtypes}")
    logging.info(f"First few rows:\n{df.head().to_string()}")

def explore_database_tables(conn):
    """Explore tables in the database."""
    tables = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{SCHEMA_NAME}'").fetchall()
    logging.info(f"Tables in '{SCHEMA_NAME}' schema:")
    for table in tables:
        logging.info(table[0])
    return tables

def explore_table_structure(conn, table_name):
    """Explore the structure of a specific table."""
    result = conn.execute(f"DESCRIBE {SCHEMA_NAME}.{table_name}").fetchall()
    logging.info(f"Table structure of '{table_name}':")
    for row in result:
        logging.info(row)

def explore_table_data(conn, table_name):
    """Explore the data in a specific table."""
    data = conn.execute(f"SELECT * FROM {SCHEMA_NAME}.{table_name} LIMIT 5").fetchall()
    columns = [desc[0] for desc in conn.description]
    logging.info(f"First rows of the '{table_name}' table:")
    for row in data:
        logging.info(dict(zip(columns, row)))

    all_data = conn.execute(f"SELECT * FROM {SCHEMA_NAME}.{table_name}").fetchall()
    print(f"All data from '{table_name}':")
    print(tabulate(all_data, headers=columns, tablefmt="grid"))

def check_table_schema(conn, table_name):
    """Check the schema of a specific table."""
    try:
        schema = conn.execute(f"DESCRIBE {SCHEMA_NAME}.{table_name}").fetchall()
        logging.info(f"Schema of '{table_name}':")
        for column in schema:
            logging.info(f"Column: {column[0]}, Type: {column[1]}")
        return len(schema)
    except Exception as e:
        logging.error(f"Error checking schema of '{table_name}': {str(e)}")
        return 0

def explore_database():
    """Main function to explore the database."""
    df = load_csv_data()
    explore_csv_data(df)

    with connect_to_db() as conn:
        tables = explore_database_tables(conn)
        
        for table in tables:
            table_name = table[0]
            try:
                column_count = check_table_schema(conn, table_name)
                logging.info(f"Table '{table_name}' has {column_count} columns.")
                explore_table_data(conn, table_name)
            except Exception as e:
                logging.error(f"Error exploring table '{table_name}': {str(e)}")

        if not tables:
            logging.info(f"No tables found in the '{SCHEMA_NAME}' schema.")

def main():
    setup_logging()
    choice = input("Enter 'clean' to clean the database and perform a deep clean, or 'explore' to explore the data: ").lower()
    if choice == 'clean':
        clean_database()
    elif choice == 'explore':
        explore_database()
    else:
        print("Invalid choice. Please enter 'clean' or 'explore'.")

    with connect_to_db() as conn:
        tables = explore_database_tables(conn)
        for table in tables:
            table_name = table[0]
            column_count = check_table_schema(conn, table_name)
            logging.info(f"Table '{table_name}' has {column_count} columns.")

    df = load_csv_data()
    logging.info(f"CSV data has {len(df.columns)} columns: {df.columns.tolist()}")

if __name__ == "__main__":
    main()