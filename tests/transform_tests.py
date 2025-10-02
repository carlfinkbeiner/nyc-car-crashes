from etl.transform_clean import cast_and_normalize, read_landing_pages, validate_rows

dir = r"/Users/carlfinkbeiner/repos/nyc-car-crashes/landing/2025-10-01_005136_cec218c8-c182-40e9-8a93-08759b67bd24"

data = read_landing_pages(dir)

normalized_data = cast_and_normalize(data)

valid_rows, invalid_rows = validate_rows(normalized_data)

print(len(valid_rows))
print(invalid_rows)
