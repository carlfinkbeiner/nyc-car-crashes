import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

with open("/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml") as f:
    config = yaml.safe_load(f)

base_url = config["dataset"]["base_url"]
dataset_id = config["dataset"]["dataset_id"]


def build_export_url(dataset_id: str, base_url: str, format: str = "csv") -> str:
    export_url = (
        f"https://{base_url}/api/views/{dataset_id}/rows.{format}?accessType=DOWNLOAD"
    )
    return export_url


def fetch_max_updated_at(dataset_id, base_url, app_token):
    headers = {"X-App-Token": app_token}
    params = {"$select": "max(:updated_at) as max_updated_at"}
    url = f"https://{base_url}/resource/{dataset_id}.json"

    with requests.get(url=url, headers=headers, params=params) as r:
        resp = r.json()
        return resp[0]["max_updated_at"]


def fetch_export(url: str, app_token: str, dest_path: str, chunk_size: int = 8192):
    headers = {"X-App-Token": app_token}
    started = datetime.now(timezone.utc)
    dest = Path(dest_path)

    h = hashlib.sha256()
    file_size = 0
    with requests.get(url=url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    h.update(chunk)
                    file_size += len(chunk)
                else:
                    continue
    suggested_next_watermark = fetch_max_updated_at(
        dataset_id=dataset_id, base_url=base_url, app_token=app_token
    )

    finished = datetime.now(timezone.utc)

    return {
        "path": str(dest),
        "bytes": file_size,
        "sha256": h.hexdigest(),
        "snapshot_time": started.isoformat(),
        "started": started.isoformat(),
        "finished": finished.isoformat(),
        "suggested_next_watermark": suggested_next_watermark,
    }


def build_incremental_params(last_watermark: str, page_limit: int) -> dict:
    params = {
        "$select": ":updated_at, collision_id, crash_date, crash_time, borough, latitude, longitude, number_of_persons_injured, number_of_persons_killed",
        "$where": f":updated_at > '{last_watermark}'",
        "$order": ":updated_at, collision_id",
        "$limit": page_limit,
    }
    return params


def fetch_page(
    dataset_id: str, base_url: str, params: dict, app_token: str, offset: int
):
    url = f"https://{base_url}/resource/{dataset_id}.json"
    headers = {"X-App-Token": app_token}
    query_params = dict(params)
    query_params["$offset"] = offset

    with requests.get(url=url, headers=headers, params=query_params) as r:
        r.raise_for_status()
        rows = r.json()
        row_count = len(rows)
        if row_count > 0:
            min_updated_at = min([row[":updated_at"] for row in rows])
            max_updated_at = max([row[":updated_at"] for row in rows])
        else:
            min_updated_at = None
            max_updated_at = None
        # print(rows[0])
        # for row in rows:
        # print(row["number_of_persons_injured"])
    return {
        "rows": rows,
        "row_count": row_count,
        "min_updated_at": min_updated_at,
        "max_updated_at": max_updated_at,
    }


# def main():
#     last_watermark = "2025-09-18T00:00:00Z"

#     params = build_incremental_params(last_watermark=last_watermark, page_limit=1000)

#     print(
#         fetch_page(
#             dataset_id=dataset_id,
#             base_url=base_url,
#             params=params,
#             app_token=str(SOCRATA_APP_TOKEN),
#             offset=0,
#         )["rows"][0]
#     )

#     return None


# if __name__ == "__main__":
#     main()
