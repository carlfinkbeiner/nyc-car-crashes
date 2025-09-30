import gzip
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

from utils.io_helpers import write_last_watermark
from utils.socrata import (
    build_export_url,
    build_incremental_params,
    fetch_export,
    fetch_page,
)


def run_initial_export(
    dataset_id: str,
    base_url: str,
    format: str,
    app_token: str,
    dest_path: str,
    chunk_size: int,
):
    url = build_export_url(dataset_id=dataset_id, base_url=base_url, format=format)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_id = str(uuid.uuid4())
    final_directory_path = os.path.join(dest_path, f"{unique_id}_{timestamp}")
    os.mkdir(final_directory_path)

    final_export_path = os.path.join(final_directory_path, "export.csv")

    export_dict = fetch_export(
        url=url, app_token=app_token, dest_path=final_export_path, chunk_size=chunk_size
    )

    manifest_dict = {"id": unique_id, "mode": "export", **export_dict}

    with open(f"{final_directory_path}/manifest.json", "w") as f:
        json.dump(manifest_dict, f, indent=4)


def iterate_incremental_pages(
    dataset_id: str, base_url: str, app_token: str, last_watermark: str, page_limit: int
):
    offset = 0
    page_no = 1
    max_pages = 30
    params = build_incremental_params(
        last_watermark=last_watermark, page_limit=page_limit
    )
    while True:
        if page_no > max_pages:
            break

        page = fetch_page(
            dataset_id=dataset_id,
            base_url=base_url,
            params=params,
            app_token=app_token,
            offset=offset,
        )
        if page["row_count"] == 0:
            break
        yield {"page_no": page_no, **page}
        if page["row_count"] < page_limit:
            break

        offset += page_limit
        page_no += 1


def write_landing_pages(run_id: str, pages_iter, dest_dir, watermark_path):

    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    for file in os.listdir(dest_dir):
        if file == "manifest.json":
            print("Failing due to already existant manifest file")
            return None

    started_at = datetime.now(timezone.utc)
    pages = []
    suggested_next_watermark = None
    page_no = 1
    total_rows = 0
    total_bytes = 0

    for page in pages_iter:
        rows = page["rows"]
        row_count = page["row_count"]
        min_updated_at = page["min_updated_at"]
        max_updated_at = page["max_updated_at"]
        file_name = f"page_{page_no:05d}.ndjson.gz"
        file_size = 0
        file_path = os.path.join(dest_dir, file_name)

        with gzip.open(file_path, "wt", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")

        file_size = os.path.getsize(file_path)

        h = hashlib.sha256()

        with open(file_path, "rb") as fh:
            for chunk in iter(lambda: fh.read(8192), b""):
                h.update(chunk)
        sha256_hex = h.hexdigest()

        append_page_dict = {
            "page_no": page_no,
            "path": file_path,
            "rows": row_count,
            "min_updated_at": min_updated_at,
            "max_updated_at": max_updated_at,
            "sha256": sha256_hex,
            "bytes": file_size,
        }

        pages.append(append_page_dict)
        page_no += 1
        total_rows += row_count
        total_bytes += file_size
        if max_updated_at is not None:
            if suggested_next_watermark is None:
                suggested_next_watermark = max_updated_at
            else:
                suggested_next_watermark = max(suggested_next_watermark, max_updated_at)

    finished_at = datetime.now(timezone.utc)

    manifest = {
        "schema_version": "1.0",
        "run_id": run_id,
        "pages": pages,
        "totals": {
            "total_rows": total_rows,
            "total_size": total_bytes,
            "total_pages": len(pages),
        },
        "suggested_next_watermark": suggested_next_watermark,
        "started_at": started_at,
        "finished_at": finished_at,
    }

    with open(os.path.join(dest_dir, "manifest.json"), "w") as mf:
        json.dump(manifest, mf, indent=4)

    write_last_watermark(manifest=manifest, watermark_path=watermark_path)

    return manifest


# # CONFIG FOR TESTING--------------------------------------------------------------------
# load_dotenv()

# with open("/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml") as f:
#     config = yaml.safe_load(f)

# base_url = config["dataset"]["base_url"]
# dataset_id = config["dataset"]["dataset_id"]
# SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
# dest_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/landing"

# run_initial_export(
#     dataset_id=dataset_id,
#     base_url=base_url,
#     format="csv",
#     app_token=str(SOCRATA_APP_TOKEN),
#     dest_path=dest_path,
#     chunk_size=8192,
# )
