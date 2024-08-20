import sys
import logging
import pandas as pd
import dlt
from yato import Yato
import duckdb
import json

sys.argv = [sys.argv[0]]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def fetch_avocats_data():
    file_path = 'data/nombre-par-barreau.csv'
    try:
        data = pd.read_csv(file_path)
        logging.info(f"Loaded {len(data)} records from {file_path}")
        
        # Convert year columns to numeric
        for year in range(2002, 2023):
            col_name = f'avocats_{year}'
            data[col_name] = pd.to_numeric(data[col_name], errors='coerce')
        
        # Log the first few rows and columns for verification
        logging.info(f"First few rows of data:\n{data.head().to_string()}")
        logging.info(f"Columns in the DataFrame: {data.columns.tolist()}")
        
        return data
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {str(e)}")
        return None

def check_database_structure(db_path):
    try:
        conn = duckdb.connect(db_path)
        schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        logging.info(f"Schemas in the database: {schemas}")
        
        for schema in schemas:
            tables = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema[0]}'").fetchall()
            logging.info(f"Tables in schema {schema[0]}: {tables}")
            
            for table in tables:
                columns = conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema[0]}' AND table_name = '{table[0]}'").fetchall()
                logging.info(f"Columns in {schema[0]}.{table[0]}: {columns}")
        
        conn.close()
    except Exception as e:
        logging.error(f"Error checking database structure: {str(e)}")

def cleanup_database(pipeline):
    try:
        pipeline.drop_table("avocats_data")
        logging.info("Dropped existing avocats_data table")
    except Exception as e:
        logging.warning(f"Error dropping table: {str(e)}")

print("Starting the pipeline...")
logging.info("Initializing Yato...")

yato = Yato(
    database_path="avocats_evolution.duckdb",
    sql_folder="sql/",
    schema="transform"
)

logging.info("Initializing DLT pipeline...")
pipeline = dlt.pipeline(
    pipeline_name="avocats_evolution",
    destination="duckdb",
    dataset_name="avocats"  # This will be the schema name
)

logging.info("Cleaning up existing data...")
cleanup_database(pipeline)

logging.info("Fetching avocats data...")
data = fetch_avocats_data()

if data is not None:
    try:
        # Define the schema based on the DataFrame columns
        schema = {col: dlt.String() if col in ['name', 'address', 'cour_appel', 'site_internet', 'numero_telephone'] 
                  else dlt.Float() for col in data.columns}

        # Log the schema
        logging.info(f"Schema: {schema}")

        # Create the table with the defined schema
        pipeline.create_table("avocats_data", schema=schema)

        # Convert DataFrame to records
        records = data.to_dict('records')
        logging.info(f"Sample record: {records[0]}")

        # Append data to the table
        info = pipeline.append("avocats_data", records)
        print("Data appended to table. Info:")
        print(info)
        logging.info(f"Append info: {info}")

    except Exception as e:
        logging.error(f"Error running pipeline: {str(e)}")
else:
    logging.error("No data to process.")

logging.info("Checking database structure after pipeline run...")
check_database_structure("avocats_evolution.duckdb")

logging.info("Running Yato transformations...")
try:
    yato.run()
    logging.info("Yato transformations completed successfully.")
    
    conn = duckdb.connect("avocats_evolution.duckdb")
    result = conn.execute("SELECT avocats_analysis_json FROM 'avocats_analysis.json'").fetchone()
    
    with open('avocats_analysis.json', 'w') as f:
        json.dump(json.loads(result[0]), f)
    
    logging.info("JSON file generated successfully.")
    conn.close()

except Exception as e:
    logging.error(f"Error running Yato: {str(e)}")

logging.info("Checking database structure after Yato transformations...")
check_database_structure("avocats_evolution.duckdb")

print("Pipeline execution completed.")