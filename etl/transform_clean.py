import gzip
import json
import os


def read_landing_pages(run_folder: str):
    rows = []
    for filename in os.listdir(run_folder):
        if not filename.endswith(".ndjson.gz"):
            continue
        file_path = os.path.join(run_folder, filename)

        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                rows.append(record)

    return rows
