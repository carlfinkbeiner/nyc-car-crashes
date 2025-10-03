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


########FINAL IMPLEMENTATION TO INCLUDE#####################
# initial_export_manifest = run_initial_export(
#     dataset_id=dataset_id,
#     base_url=base_url,
#     format="csv",
#     app_token=app_token,
#     dest_path=landing_path,
#     chunk_size=8192,
#     watermark_path=watermark_path,
# )
# landing_folder = initial_export_manifest["landing_folder"]


# Hardcoded for testing purposes
# Runs but all rows invalid, need to dbut
landing_folder = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/landing/2025-10-02_221954_d478f920-ddff-4271-8a16-7a57117b6681"
run_transform(landing_folder=landing_folder, transform_dir=transform_dir)
