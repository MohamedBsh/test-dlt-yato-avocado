import sys
import logging
import pandas as pd
import dlt
from yato import Yato
import duckdb

sys.argv = [sys.argv[0]]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_avocats_data():
    file_path = 'data/nombre-par-barreau.csv'
    try:
        data = pd.read_csv(file_path, sep=';')
        logging.info(f"Loaded {len(data)} records from {file_path}")
        return data.to_dict(orient='records')
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {str(e)}")
        return None

def check_tables_in_duckdb(db_path):
    """Check and log the tables in the DuckDB database."""
    try:
        conn = duckdb.connect(db_path)

        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
        tables = conn.execute(query).fetchall()

        logging.info("Tables in the database:")
        for table in tables:
            logging.info(table[0])
        
        conn.close()

    except Exception as e:
        logging.error(f"Error checking tables in DuckDB: {str(e)}")

def query_avocats_data(db_path, query):
    """Execute a query on the DuckDB database and return results."""
    try:
        conn = duckdb.connect(db_path)

        results = conn.execute(query).fetchdf()
        
        conn.close()

        logging.info(f"Query results:\n{results}")

        return results

    except Exception as e:
        logging.error(f"Error querying data from DuckDB: {str(e)}")
        return None

yato = Yato(
    database_path="avocats_evolution.duckdb",
    sql_folder="sql/",
    schema="transform"
)

pipeline = dlt.pipeline(pipeline_name="avocats_evolution", destination="duckdb", dataset_name="avocats_data")

data = fetch_avocats_data()

if data:
    try:
        resource = dlt.resource(data, name='avocats_data')
        
        info = pipeline.run([resource])
        print(info)
        logging.info(f"Pipeline run info: {info}")

        load_info = pipeline.run(data, table_name='avocats_data')
        logging.info(f"Load info: {load_info}")

    except Exception as e:
        logging.error(f"Error running pipeline: {str(e)}")
else:
    logging.error("No data to process.")



try:
    yato.run()
    logging.info("Yato transformations completed successfully.")
except Exception as e:
    logging.error(f"Error running Yato: {str(e)}")


query = "SELECT * FROM avocats_data LIMIT 10;"
results = query_avocats_data('avocats_evolution.duckdb', query)
print(results)