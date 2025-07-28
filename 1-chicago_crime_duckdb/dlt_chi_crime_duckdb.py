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
    limit = 1000
    offset = 0
    while True:
        url = f"https://data.cityofchicago.org/resource/ijzp-q8t2.json?$where=year=2025&$limit={limit}&$offset={offset}"
        data = requests.get(url, headers=headers).json()
        if not data:
            break
        yield data
        offset += limit

pipeline = dlt.pipeline(
    pipeline_name="chicago_crime_pipeline",
    destination="duckdb",
    dataset_name="chicago_crime_data",
    pipelines_dir="./myconfig"
)

load_info = pipeline.run(fetch_crime_data(), loader_file_format="jsonl")
print(f"Loaded: {load_info}")
