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
        """
        CREATE OR REPLACE TEMP TABLE staging_new_crashes AS
        SELECT
            collision_id,
            crash_ts,
            borough,
            latitude,
            longitude,
            number_of_persons_injured,
            number_of_persons_killed,
            updated_at
        FROM read_parquet(?);
        """,
        [transformed_parquet_path],
    )

    connection.sql(
        """
        INSERT OR REPLACE INTO crashes
        SELECT
            collision_id,
            crash_ts,
            borough,
            latitude,
            longitude,
            number_of_persons_injured,
            number_of_persons_killed
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY collision_id
                    ORDER BY updated_at DESC NULLS LAST, crash_ts DESC NULLS LAST
                ) AS row_rank
            FROM staging_new_crashes
        )
        WHERE row_rank = 1;
        """
    )

    connection.sql("DROP TABLE staging_new_crashes;")
