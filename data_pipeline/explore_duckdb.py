import duckdb
import pandas as pd

def explore_duckdb(db_path):
    con = duckdb.connect(db_path, read_only=True)

    print(f"Exploring database: {db_path}")

    schemas = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    print("\nSchemas in the database:", [schema[0] for schema in schemas])

    def list_tables(schema):
        tables = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}'").fetchall()
        return [table[0] for table in tables]

    def preview_table(schema_name, table_name, num_rows=5):
        query = f"SELECT * FROM {schema_name}.{table_name} LIMIT {num_rows}"
        df = con.execute(query).fetchdf()
        print(f"\nPreview of table {schema_name}.{table_name}:")
        print(df)

    for schema in schemas:
        schema_name = schema[0]
        print(f"\nTables in the {schema_name} schema:")
        tables = list_tables(schema_name)
        print(tables)

        for table in tables:
            preview_table(schema_name, table)

    con.close()

explore_duckdb('avocats_evolution.duckdb')