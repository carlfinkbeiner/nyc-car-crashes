import json

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

    with open(watermark_path, "w") as f:
        json.dump(watermark_dict, f, indent=4)

    return True


def load_last_watermark(watermark_path: str):
    with open(watermark_path, "r") as f:
        data = json.load(f)
        last_watermark = data["last_watermark"]

        return last_watermark


def safe_load_yaml(yaml_path: str):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)
