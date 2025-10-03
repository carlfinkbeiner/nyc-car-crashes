from orchestration.run_incremental_ingest import main as run_incremental_ingest
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
landing_path = config["landing_dir"]
watermark_path = config["state_file_path"]
transform_dir = config["transform_dir"]
database_path = config["database_path"]

# INCREMENTAL INGESTION
print("Starting incremental_ingest.....")
incremental_ingest_manigest = run_incremental_ingest(
    settings_path=settings_path,
    secrets_path=secrets_path,
    override_watermark=None,
    override_page_limit=None,
    dry_run=False,
)

rows_written = incremental_ingest_manigest["rows_written"]
print("Incremental ingestion complete")
print(f"Rows written: {rows_written}\n")

# TRANSFORMATION
print("Starting transform.....")
landing_folder = incremental_ingest_manigest["landing_folder"]
transform_manifest = run_transform(
    landing_folder=landing_folder, transform_dir=transform_dir
)
print("Transformation complete")

# LOAD
print("Starting load.....")
transformed_parquet_path = transform_manifest["transformed_parquet_path"]
run_load(database_path=database_path, transformed_parquet_path=transformed_parquet_path)
