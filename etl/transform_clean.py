import gzip
import json
import os
from datetime import datetime

import pandas as pd


def read_landing_pages(landing_folder: str):
    rows = []
    for filename in os.listdir(landing_folder):
        if not filename.endswith(".ndjson.gz"):
            continue
        file_path = os.path.join(landing_folder, filename)

        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                clean_record = {
                    k.lstrip(":").lower().replace(" ", "_"): v
                    for k, v in record.items()
                }
                rows.append(clean_record)

    return rows, landing_folder


def _g(row: dict, key: str, default=None):
    return row.get(key, default)


def _safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _safe_str(value):
    try:
        return str(value)
    except (ValueError, TypeError):
        return None


def _safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_time(value):
    try:
        return datetime.strptime(value, "%H:%M").time()
    except (ValueError, TypeError):
        return None


def _parse_updated_at(value):
    if value is None:
        return None
    else:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None


def _parse_crash_date(value):
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        pass
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            d = datetime.strptime(value, fmt)
            return datetime(d.year, d.month, d.day)
        except ValueError:
            continue
    return None


def _parse_crash_ts(datepart, timepart):
    d = _parse_crash_date(datepart)  # use the tolerant date parser
    t = _safe_time(timepart)  # use the tolerant time parser
    if d is None:
        return None
    if t is None:

        return d
    return datetime.combine(d.date(), t)


def _parse_borough(value):
    if value is None:
        return None
    try:
        return str(value)
    except (TypeError, ValueError):
        return None


def cast_and_normalize(rows: list[dict]):
    normalized_rows = []
    for row in rows:
        record = {
            "updated_at": _parse_updated_at(_g(row, "updated_at")),
            "collision_id": _safe_str(_g(row, "collision_id")),
            "crash_date": _parse_crash_date(_g(row, "crash_date")),
            "crash_time": _safe_time(_g(row, "crash_time")),
            "crash_ts": _parse_crash_ts(_g(row, "crash_date"), _g(row, "crash_time")),
            "borough": _parse_borough(_g(row, "borough")),
            "latitude": _safe_float(_g(row, "latitude")),
            "longitude": _safe_float(_g(row, "longitude")),
            "number_of_persons_injured": _safe_int(
                _g(row, "number_of_persons_injured")
            ),
            "number_of_persons_killed": _safe_int(_g(row, "number_of_persons_killed")),
        }
        # print(record)
        normalized_rows.append(record)

    return normalized_rows


def validate_rows(normalized_rows: list[dict]):
    valid_rows = []
    invalid_rows = []

    for row in normalized_rows:

        invalid_reasons = []
        required_fields = [
            "collision_id",
            "crash_ts",
            "latitude",
            "longitude",
            "number_of_persons_injured",
            "number_of_persons_killed",
        ]
        for field in required_fields:
            if row.get(field) is None:
                invalid_reasons.append(field)

        if invalid_reasons:
            invalid_record = {"row": row, "invalid_reasond": invalid_reasons}
            invalid_rows.append(invalid_record)

        else:
            valid_rows.append(row)
    return valid_rows, invalid_rows


def write_transformed(valid_rows: list[dict], transform_dir: str, landing_folder: str):
    with open(os.path.join(landing_folder, "manifest.json"), "r") as f:
        landing_manifest = json.load(f)
        landing_run_id = landing_manifest["run_id"]

    transformed_data_path = os.path.join(transform_dir, landing_run_id)

    if not os.path.isdir(transformed_data_path):
        os.mkdir(transformed_data_path)

    df = pd.DataFrame(valid_rows)
    transformed_parquet_path = os.path.join(
        transformed_data_path, "transformed_data.parquet"
    )
    df.to_parquet(transformed_parquet_path)
    created_at = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    row_count = len(valid_rows)

    transform_manifest = {
        "row_count": row_count,
        "run_id": landing_run_id,
        "created_at": created_at,
        "source_landing_folder": landing_folder,
        "transform_folder": transformed_data_path,
        "transformed_parquet_path": transformed_parquet_path,
    }

    return transform_manifest, transformed_parquet_path
