import os
import dlt
import dotenv
from dlt.sources.helpers import requests

dotenv.load_dotenv()

CHICAGO_CRIME_API = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$where=year=2025"

CHI_APP_TOKEN = os.getenv("CHI_APP_TOKEN")

@dlt.resource(name="chicago_crime", write_disposition="merge", primary_key="id")
def fetch_crime_data():
    headers = {
        "X-App-Token": f"{CHI_APP_TOKEN}"
    }
    limit = 50000
    offset = 0
    while True:
        url = f"https://data.cityofchicago.org/resource/ijzp-q8t2.json?$where=year=2025&$limit={limit}&$offset={offset}&$order=id"
        data = requests.get(url, headers=headers).json()
        if not data:
            break
        # cleaned = []
        # for row in data:
        #     try:
        #         if 'id' in row and row['id']:
        #             row['id'] = int(row['id']) 
        #             cleaned.append(row)
        #     except Exception as e:
        #         print(f"Skipping row due to error: {e}, row: {row}")
        
        yield data
        # break
        offset += limit


# chicago_crime_resource = dlt.resource(
#     fetch_crime_data,
#     name="chicago_crime",
#     write_disposition="merge",
#     primary_key="id"
# )

pipeline = dlt.pipeline(
    # import_schema_path="schemas/import",
    # export_schema_path="schemas/export",
    pipeline_name="chicago_crime_pipeline",
    destination="duckdb",
    dataset_name="chicago_crime_data"
    # pipelines_dir="./myconfig"
    # ,dev_mode=True
)
# load_info = pipeline.run(chicago_crime_resource, loader_file_format="jsonl")
load_info = pipeline.run(fetch_crime_data(), loader_file_format="jsonl")
print(f"Loaded: {load_info}")
