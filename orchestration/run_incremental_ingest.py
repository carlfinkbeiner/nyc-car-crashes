import os
import uuid
from datetime import datetime, timedelta, timezone

from etl.extract_crashes import iterate_incremental_pages, write_landing_pages
from utils.io_helpers import load_last_watermark, safe_load_yaml, write_last_watermark


def main(
    settings_path: str,
    secrets_path: str,
    override_watermark: str | None,
    override_page_limit: int | None,
    dry_run: bool,
):
    settings = safe_load_yaml(settings_path)
    secrets = safe_load_yaml(secrets_path)

    dataset_id = settings["dataset"]["dataset_id"]
    base_url = settings["dataset"]["base_url"]
    app_token = secrets["socrata"]["app_token"]
    landing_dir = settings["landing_dir"]
    state_file_path = settings["state_file_path"]

    # resolve last watermark
    if override_watermark:
        last_watermark = override_watermark
    elif state_file_path:
        last_watermark = load_last_watermark(state_file_path)
    else:
        last_watermark = str(datetime.now() - timedelta(days=1))

    # resolve page limit
    if override_page_limit:
        page_limit = override_page_limit
    else:
        page_limit = settings["dataset"]["page_limit"]

    # make landing directory
    run_id = str(uuid.uuid4())
    timestamp_formatted = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    landing_path = os.path.join(landing_dir, f"{timestamp_formatted}_{run_id}")
    os.mkdir(landing_path)

    if dry_run:
        pages_iter = iterate_incremental_pages(
            dataset_id=dataset_id,
            base_url=base_url,
            app_token=app_token,
            last_watermark=last_watermark,
            page_limit=page_limit,
        )
        first = next(pages_iter, None)
        return {
            "exit_code": 0,
            "run_id": run_id,
            "run_folder": landing_path,
            "probe": (
                None
                if first is None
                else {
                    k: first[k]
                    for k in (
                        "page_no",
                        "row_count",
                        "min_updated_at",
                        "max_updated_at",
                    )
                }
            ),
        }

    # iterate and land pages
    pages_iter = iterate_incremental_pages(
        dataset_id=dataset_id,
        base_url=base_url,
        app_token=app_token,
        last_watermark=last_watermark,
        page_limit=page_limit,
    )

    manifest = write_landing_pages(
        run_id=run_id,
        pages_iter=pages_iter,
        dest_dir=landing_path,
        watermark_path=state_file_path,
    )

    if manifest is None:
        return {
            "exit_code": 1,
            "run_id": run_id,
            "run_folder": landing_path,
            "error": "write_landing_pages returned None (likely manifest already exists or write failed).",
        }

    suggested_next_watermark = manifest["suggested_next_watermark"]

    if suggested_next_watermark:
        write_last_watermark(manifest=manifest, watermark_path=state_file_path)

    return {
        "exit_code": 0,
        "run_id": run_id,
        "run_folder": landing_path,
        "suggested_next_watermark": suggested_next_watermark,
        "pages_written": len(manifest.get("pages", [])),
        "rows_written": sum(p.get("rows", 0) for p in manifest.get("pages", [])),
    }


if __name__ == "__main__":
    settings_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml"
    secrets_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/config/secrets.yaml"
    override_watermark = "2025-09-28T00:00:00.000Z"
    override_page_limit = 10
    main(
        settings_path=settings_path,
        secrets_path=secrets_path,
        override_watermark=override_watermark,
        override_page_limit=override_page_limit,
        dry_run=False,
    )
