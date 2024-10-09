import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
from app.config import DB_PATH, SCHEMA_NAME, TABLE_NAME, PLOT_TITLE, PLOT_X_AXIS, PLOT_Y_AXIS, PLOT_WIDTH, PLOT_HEIGHT, PLOT_OUTPUT

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_db(db_path):
    """Establish a connection to the database."""
    return duckdb.connect(db_path)

def fetch_data(conn, schema, table):
    """Fetch data from the database."""
    query = f"SELECT * FROM {schema}.{table} LIMIT 1"
    return conn.execute(query).fetchone()

def prepare_plot_data(result):
    """Prepare data for plotting."""
    years = list(range(2002, 2023))
    counts = [result[i] for i in range(len(years))]
    logging.info(f"Years: {years}")
    logging.info(f"Counts: {counts}")
    return years, counts

def create_plot(years, counts, title, x_axis, y_axis, width, height):
    """Create the plot using Plotly."""
    fig = make_subplots(rows=1, cols=1)
    fig.add_trace(go.Scatter(
        x=years, 
        y=counts, 
        mode='lines+markers+text',
        text=counts,
        textposition='top center'
    ))

    fig.update_layout(
        title=title,
        xaxis_title=x_axis,
        yaxis_title=y_axis,
        width=width,
        height=height
    )
    return fig

def save_plot(fig, output_path):
    """Save the plot as an image."""
    fig.write_image(output_path)
    logging.info(f"Plot successfully generated in {output_path}")

def generate_plot():
    """Main function to generate the plot."""
    setup_logging()
    
    with connect_to_db(DB_PATH) as conn:
        result = fetch_data(conn, SCHEMA_NAME, TABLE_NAME)
        
        logging.info("Content of result:")
        logging.info(result)
        logging.info(f"Type of result: {type(result)}")
        
        years, counts = prepare_plot_data(result)
        
        fig = create_plot(years, counts, PLOT_TITLE, PLOT_X_AXIS, PLOT_Y_AXIS, PLOT_WIDTH, PLOT_HEIGHT)
        
        save_plot(fig, PLOT_OUTPUT)

if __name__ == "__main__":
    generate_plot()