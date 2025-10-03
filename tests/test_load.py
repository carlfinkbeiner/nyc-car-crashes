from etl.load_upsert import (
    connect_to_duckdb,
)

db = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/database/crashes_db.duckdb"
parquet_file = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/transform/cec218c8-c182-40e9-8a93-08759b67bd24/transformed_data.parquet"

connection = connect_to_duckdb(db)

# create_crashes_table(connection=connection)


# load_transformed_file(connection=connection, parquet_file=parquet_file)

print(connection.sql("SELECT * FROM crashes limit 10000;").fetchall())
