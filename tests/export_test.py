import yaml

from etl.extract_crashes import run_initial_export

with open("/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml") as f:
    config = yaml.safe_load(f)

with open("/Users/carlfinkbeiner/repos/nyc-car-crashes/config/secrets.yaml") as fs:
    secrets = yaml.safe_load(fs)

base_url = config["dataset"]["base_url"]
dataset_id = config["dataset"]["dataset_id"]
app_token = secrets["socrata"]["app_token"]
dest_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/landing"
watermark_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/state/watermark.json"

run_initial_export(
    dataset_id=dataset_id,
    base_url=base_url,
    format="csv",
    app_token=str(app_token),
    dest_path=dest_path,
    chunk_size=8192,
    watermark_path=watermark_path,
)
