from etl.extract_crashes import run_initial_export
from orchestration.run_load import main as run_load
from orchestration.run_transform import main as run_transform
from utils.io_helpers import safe_load_yaml

secrets_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/config/secrets.yaml"
settings_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml"

secrets = safe_load_yaml(secrets_path)
config = safe_load_yaml(settings_path)

base_url = config["dataset"]["base_url"]
dataset_id = config["dataset"]["dataset_id"]
app_token = secrets["socrata"]["app_token"]
landing_dir = config["landing_dir"]
watermark_path = config["state_file_path"]
transform_dir = config["transform_dir"]
database_path = config["database_path"]


print("Starting extraction.....")
initial_export_manifest = run_initial_export(
    dataset_id=dataset_id,
    base_url=base_url,
    format="csv",
    app_token=app_token,
    dest_path=landing_dir,
    chunk_size=8192,
    watermark_path=watermark_path,
)
print("Extraction complete")
landing_folder = initial_export_manifest["landing_folder"]


print("Starting transformation.....")
transform_manifest = run_transform(
    landing_folder=landing_folder, transform_dir=transform_dir
)
transformed_parquet_path = transform_manifest["transformed_parquet_path"]
print("Transformation complete")


print("Starting load to duckdb.....")
run_load(database_path=database_path, transformed_parquet_path=transformed_parquet_path)
print("Load complete")
