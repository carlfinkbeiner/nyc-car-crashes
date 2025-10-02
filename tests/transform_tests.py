from etl.transform_clean import (
    cast_and_normalize,
    read_landing_pages,
    validate_rows,
    write_transformed,
)
from utils.io_helpers import safe_load_yaml

# CONFIG----------------------------------
settings_path = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/config/settings.yaml"
settings = safe_load_yaml(settings_path)
transform_dir = settings["transform_dir"]


dir = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/landing/2025-10-01_005136_cec218c8-c182-40e9-8a93-08759b67bd24"

data, landing_folder = read_landing_pages(dir)

normalized_data = cast_and_normalize(data)

valid_rows, invalid_rows = validate_rows(normalized_data)

print(len(valid_rows))
print(invalid_rows)


transform_manifest = write_transformed(
    valid_rows=valid_rows, transform_dir=transform_dir, landing_folder=landing_folder
)

print(transform_manifest)
