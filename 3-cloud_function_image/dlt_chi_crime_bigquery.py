import os
import json
import dlt
import dotenv
from dlt.sources.helpers import requests

import functions_framework

dotenv.load_dotenv()

CHICAGO_CRIME_API = "https://data.cityofchicago.org/resource/ijzp-q8t2.json?$where=year=2025"

CHI_APP_TOKEN = os.getenv("CHI_APP_TOKEN")

with open("bq_creds.json") as f:
    creds = json.load(f)

# Set required fields as environment variables (example for DLT)
os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = creds["project_id"]
os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = creds["private_key"]
os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = creds["client_email"]

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



@functions_framework.http
def run_pipeline(request):
    try:
        pipeline = dlt.pipeline(
            pipeline_name="chicago_crime_pipeline",
            destination="bigquery",
            dataset_name="chicago_crime_data",
            pipelines_dir="./myconfig"
        )
        load_info = pipeline.run(fetch_crime_data(), loader_file_format="jsonl")
        return f"Loaded: {load_info}", 200
    except Exception as e:
        return f"Error: {str(e)}", 500
    
    

