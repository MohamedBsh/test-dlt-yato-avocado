import duckdb
import dlt
from yato import Yato
import pandas as pd
from datetime import datetime
import requests
import io
import logging
import os
from config import (
    URL, DB_PATH, SQL_FOLDER, SCHEMA_NAME, PIPELINE_NAME,
    DESTINATION, DATASET_NAME, TABLE_NAME
)

def load_csv_data(url):
    """Load CSV data from a given URL."""
    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text))

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_yato(db_path, sql_folder, schema):
    """Initialize and return a Yato instance."""
    yato = Yato(database_path=db_path, sql_folder=sql_folder, schema=schema)
    logging.info(f"SQL folder path: {os.path.abspath(yato.sql_folder)}")
    return yato

def process_sql_files(sql_folder):
    """Process SQL files in the given folder."""
    if not os.path.exists(sql_folder):
        logging.error(f"SQL folder does not exist: {sql_folder}")
        return []

    sql_files = [os.path.join(root, file) 
                 for root, _, files in os.walk(sql_folder) 
                 for file in files if file.endswith('.sql')]
    
    logging.info(f"SQL files found: {sql_files}")

    for sql_file in sql_files:
        if os.path.isfile(sql_file):
            with open(sql_file, 'r') as f:
                content = f.read()
                logging.info(f"Content of {os.path.basename(sql_file)}:\n{content}")
        else:
            logging.warning(f"Skipping non-file: {sql_file}")

    return sql_files

def create_dlt_pipeline(name, destination, dataset):
    """Create and return a DLT pipeline."""
    logging.info("Initializing DLT pipeline...")
    return dlt.pipeline(pipeline_name=name, destination=destination, dataset_name=dataset)

def process_data(pipeline, data):
    """Process the data using the given pipeline."""
    if data is None or data.empty:
        logging.error("No data to process.")
        return

    try:
        schema = {col: dlt.String() if col in ['name', 'address', 'cour_appel', 'site_internet', 'numero_telephone'] 
                  else dlt.Float() for col in data.columns}
        logging.info(f"Schema: {schema}")

        pipeline.create_table(TABLE_NAME, schema=schema)
        records = data.to_dict('records')
        logging.info(f"Sample record: {records[0]}")

        info = pipeline.append(TABLE_NAME, records)
        logging.info(f"Data appended to table. Info: {info}")

    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")

def run_yato(yato, db_path, schema):
    """Run Yato operations."""
    logging.info("Running Yato...")
    try:
        with duckdb.connect(db_path) as con:
            con.execute(f"SET search_path = '{schema}'")
            yato.run()
        logging.info("Yato run completed successfully")
    except Exception as e:
        logging.error(f"Error running Yato: {str(e)}")
        raise

def run_pipeline():
    setup_logging()
    logging.info("Starting pipeline")

    yato = initialize_yato(DB_PATH, SQL_FOLDER, SCHEMA_NAME)
    process_sql_files(yato.sql_folder)

    pipeline = create_dlt_pipeline(PIPELINE_NAME, DESTINATION, DATASET_NAME)
    
    data = load_csv_data(URL)
    logging.info(f"Data loaded, shape: {data.shape}")

    load_info = pipeline.run(data, table_name=TABLE_NAME)
    logging.info(f"Pipeline run completed, load_info: {load_info}")

    process_data(pipeline, data)
    
    run_yato(yato, DB_PATH, SCHEMA_NAME)

    logging.info(f"Pipeline successfully executed on {datetime.now()}")

if __name__ == "__main__":
    run_pipeline()