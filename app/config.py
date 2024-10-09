import os

URL = "https://static.data.gouv.fr/resources/evolution-du-nombre-davocats-en-france-par-barreau/20240403-143707/nombre-par-barreau.csv"
DB_PATH = "avocats_evolution.duckdb"
SQL_FOLDER = "sql/"
SCHEMA_NAME = "avocats"
PIPELINE_NAME = "avocats_evolution"
DESTINATION = "duckdb"
DATASET_NAME = "avocats"
TABLE_NAME = "nombre_avocats"
PARQUET_DIR = os.path.join(os.path.expanduser("~"), ".dlt", "pipelines", "avocats_evolution", "load", "normalized")


# Plot configuration
PLOT_TITLE = 'Evolution of the Number of Lawyers in France between 2002 and 2022'
PLOT_X_AXIS = 'Year'
PLOT_Y_AXIS = 'Number of Lawyers'
PLOT_WIDTH = 1200
PLOT_HEIGHT = 800
PLOT_OUTPUT = "lawyers_evolution_visualization.png"