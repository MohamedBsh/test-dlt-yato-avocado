import sys
import logging
import pandas as pd
import dlt
from yato import Yato
import duckdb
from explore_duckdb import explore_avocats_data

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
        data = pd.read_csv(file_path, sep=';')
        logging.info(f"Loaded {len(data)} records from {file_path}")
        return data.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {str(e)}")
        return None

print("Starting the pipeline...")
logging.info("Initializing Yato...")

yato = Yato(
    database_path="avocats_evolution.duckdb",
    sql_folder="sql/",
    schema="transform"
)

logging.info("Initializing DLT pipeline...")
pipeline = dlt.pipeline(pipeline_name="avocats_evolution", destination="duckdb", dataset_name="avocats_data")

logging.info("Attempting to restore database...")
try:
    yato.restore()
    logging.info("Database restored successfully.")
except Exception as e:
    logging.warning(f"Failed to restore database: {str(e)}")

logging.info("Fetching avocats data...")
data = fetch_avocats_data()

if data:
    logging.info(f"Fetched {len(data)} records. Starting pipeline run...")
    try:
        resource = dlt.resource(data, name='avocats_data')
        
        info = pipeline.run([resource])
        print("Pipeline run completed. Info:")
        print(info)
        logging.info(f"Pipeline run info: {info}")

        load_info = pipeline.run(data, table_name='avocats_data')
        logging.info(f"Load info: {load_info}")

        logging.info("Attempting to backup database...")
        try:
            yato.backup()
            logging.info("Database backed up successfully.")
        except Exception as e:
            logging.warning(f"Failed to backup database: {str(e)}")

    except Exception as e:
        logging.error(f"Error running pipeline: {str(e)}")
else:
    logging.error("No data to process.")

logging.info("Running Yato transformations...")
try:
    yato.run()
    logging.info("Yato transformations completed successfully.")
except Exception as e:
    logging.error(f"Error running Yato: {str(e)}")

#explore_avocats_data('avocats_evolution.duckdb')