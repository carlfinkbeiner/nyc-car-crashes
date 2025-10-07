import csv
import gzip
import json

import duckdb
import yaml


def write_last_watermark(manifest: dict, watermark_path: str):
    last_watermark = manifest["suggested_next_watermark"]
    updated_at = manifest["finished_at"]
    run_id = manifest["run_id"]

    watermark_dict = {
        "last_watermark": last_watermark,
        "updated_at": updated_at,
        "run_id": run_id,
    }

    if last_watermark is not None:
        with open(watermark_path, "w") as f:
            json.dump(watermark_dict, f, indent=4)

    else:
        pass


def load_last_watermark(watermark_path: str):
    with open(watermark_path, "r") as f:
        data = json.load(f)
        last_watermark = data["last_watermark"]

        return last_watermark


def safe_load_yaml(yaml_path: str):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)


def csv_to_ndjson_gz(csv_file, ndjson_gz_file):
    with open(csv_file, "r", newline="", encoding="utf-8") as f_in, gzip.open(
        ndjson_gz_file, "wt", encoding="utf-8"
    ) as f_out:

        reader = csv.DictReader(f_in)
        for row in reader:
            json_line = json.dumps(row)
            f_out.write(json_line + "\n")


def query_db(database_path: str, query: str, params: tuple | None):
    with duckdb.connect(database=database_path) as con:
        result = con.sql(query=query, params=params).fetchdf()
    return result
