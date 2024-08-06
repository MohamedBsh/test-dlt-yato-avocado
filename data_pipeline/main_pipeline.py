import dlt
from yato import Yato
import pandas as pd
import requests
from io import StringIO
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL of the data
URL = "https://www.data.gouv.fr/fr/datasets/r/05f4ca76-0d72-4ecd-9f5c-20a12965e348"

def fetch_avocats_data():
    logging.info("Fetching avocats data")
    response = requests.get(URL)
    data = pd.read_csv(StringIO(response.text), sep=';')
    return data.to_dict(orient='records')

def run_pipeline():
    start_time = time.time()
    logging.info("Starting the Avocats Analysis pipeline")

    try:
        logging.info("Initializing dlt pipeline")
        pipeline = dlt.pipeline(pipeline_name="avocats_evolution", destination="duckdb", dataset_name="avocats")
        
        data = fetch_avocats_data()
        logging.info(f"Fetched {len(data)} records")
        
        logging.info("Running dlt pipeline")
        info = pipeline.run(data)
        logging.info(f"dlt pipeline completed. Info: {info}")
        
        logging.info("Initializing Yato")
        yato = Yato(
            database_path="avocats.duckdb",
            sql_folder="sql/",
            schema="analysis"
        )
        
        logging.info("Running Yato transformations")
        yato.run()
        logging.info("Yato transformations completed")
        
        logging.info("Querying analysis results")
        avocats_analysis = yato.read_sql("SELECT * FROM avocats.avocats_evolution ORDER BY name")
        logging.info(f"Retrieved {len(avocats_analysis)} rows of analysis results")
        
        logging.info("Saving results to JSON")
        avocats_analysis.to_json("avocats_analysis.json", orient="records")
        logging.info("Results saved to avocats_analysis.json")

        end_time = time.time()
        logging.info(f"Pipeline completed successfully in {end_time - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"An error occurred during pipeline execution: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run_pipeline()