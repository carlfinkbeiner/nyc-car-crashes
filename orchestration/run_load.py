import pandas as pd

from etl.load_upsert import (
    connect_to_duckdb,
    create_crashes_table,
    load_transformed_file,
)
from utils.io_helpers import safe_load_yaml


def main(database_path: str, transformed_parquet_path: str):
    connection = connect_to_duckdb(database_path=database_path)
    create_crashes_table(connection=connection)
    df = pd.read_parquet(transformed_parquet_path)
    if len(df) == 0:
        print("No rows to write")
    else:
        load_transformed_file(
            connection=connection, transformed_parquet_path=transformed_parquet_path
        )


if __name__ == "__main__":
    # CONFIG----------------
    settings_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml"
    transformed_parquet_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/transform/2025-10-07_205641_19d330dc-e888-43d3-8438-ec180aa70746/transformed_data.parquet"
    settings = safe_load_yaml(settings_path)
    database_path = settings["database_path"]
    main(database_path=database_path, transformed_parquet_path=transformed_parquet_path)
