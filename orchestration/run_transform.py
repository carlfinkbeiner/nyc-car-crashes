import json
import os

from etl.transform_clean import (
    cast_and_normalize,
    read_landing_pages,
    validate_rows,
    write_transformed,
)
from utils.io_helpers import safe_load_yaml


def main(landing_folder: str, transform_dir: str):
    rows, landing_folder = read_landing_pages(landing_folder=landing_folder)
    normalized_rows = cast_and_normalize(rows=rows)
    valid_rows, invalid_rows = validate_rows(normalized_rows=normalized_rows)
    transform_manifest, transformed_parquet_path = write_transformed(
        valid_rows=valid_rows,
        transform_dir=transform_dir,
        landing_folder=landing_folder,
    )

    out_dir = os.path.dirname(transformed_parquet_path)
    os.makedirs(out_dir, exist_ok=True)  # harmless if it already exists

    transform_manifest_path = os.path.join(out_dir, "transform_manifest.json")

    with open(transform_manifest_path, "w") as f:
        json.dump(transform_manifest, f, indent=4)

    with open(transform_manifest_path, "r") as fr:
        transform_manifest_dict = json.load(fr)
        row_count = transform_manifest_dict["row_count"]
        invalid_row_count = len(invalid_rows)
        print(f"Valid rows written to parquet: {row_count}")
        print(f"Invalid row count: {invalid_row_count}")
        print(f"Manifest path: {transform_manifest_path}")

    return transform_manifest


if __name__ == "__main__":
    settings_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml"
    landing_dir = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/landing/2025-10-01_005136_cec218c8-c182-40e9-8a93-08759b67bd24"
    settings = safe_load_yaml(settings_path)
    transform_dir = settings["transform_dir"]
    main(landing_folder=landing_dir, transform_dir=transform_dir)
