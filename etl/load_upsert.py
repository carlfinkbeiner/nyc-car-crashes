import duckdb


def connect_to_duckdb(database_path: str):
    connection = duckdb.connect(database=database_path)
    print("Connected")
    return connection


def create_crashes_table(connection):
    connection.sql(
        """CREATE TABLE IF NOT EXISTS crashes (
                collision_id VARCHAR PRIMARY KEY,
                crash_ts TIMESTAMP,
                borough VARCHAR,
                latitude FLOAT,
                longitude FLOAT,
                number_of_persons_injured INT,
                number_of_persons_killed INT
        );"""
    )


def load_transformed_file(connection, transformed_parquet_path):
    connection.sql(
        f"INSERT INTO crashes SELECT collision_id,crash_ts,borough,latitude,longitude,number_of_persons_injured,number_of_persons_killed FROM read_parquet('{transformed_parquet_path}');"
    )
