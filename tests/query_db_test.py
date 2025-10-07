from utils.io_helpers import (
    query_db,
)

db = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/database/crashes_db.duckdb"
parquet_file = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/transform/cec218c8-c182-40e9-8a93-08759b67bd24/transformed_data.parquet"

query = """SELECT * from crashes ORDER BY crash_ts desc LIMIT 100;"""

df = query_db(database_path=db, query=query, params=None)
print(df)
