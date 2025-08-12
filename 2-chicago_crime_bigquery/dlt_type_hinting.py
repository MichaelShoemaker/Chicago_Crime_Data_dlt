import os
import json
import dlt
import dotenv
from dlt.sources.helpers import requests

dotenv.load_dotenv()

# CHI_APP_TOKEN = os.getenv("CHI_APP_TOKEN")

# with open("bq_creds.json") as f:
#     creds = json.load(f)

# # BigQuery creds are harmless to keep even though we won't load
# os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PROJECT_ID"] = creds["project_id"]
# os.environ["DESTINATION__BIGQUERY__CREDENTIALS__PRIVATE_KEY"] = creds["private_key"]
# os.environ["DESTINATION__BIGQUERY__CREDENTIALS__CLIENT_EMAIL"] = creds["client_email"]

# ---- dlt column hints ----
HINTS = {
    "id": {"data_type": "bigint"},
    "case_number": {"data_type": "text"},
    "date": {"data_type": "timestamp"},
    "block": {"data_type": "text"},
    "iucr": {"data_type": "text"},
    "primary_type": {"data_type": "text"},
    "description": {"data_type": "text"},
    "location_description": {"data_type": "text"},
    "arrest": {"data_type": "bool"},
    "domestic": {"data_type": "bool"},
    "beat": {"data_type": "text"},
    "district": {"data_type": "text"},
    "ward": {"data_type": "bigint"},
    "community_area": {"data_type": "text"},
    "fbi_code": {"data_type": "text"},
    "x_coordinate": {"data_type": "double"},
    "y_coordinate": {"data_type": "double"},
    "year": {"data_type": "bigint"},
    "updated_on": {"data_type": "timestamp"},
    "latitude": {"data_type": "double"},
    "longitude": {"data_type": "double"},
    "location": {"data_type": "json"},
}

# Optional: limit to first page to keep extract fast while building schema
ONLY_FIRST_PAGE = True

@dlt.resource(name="chicago_crime", write_disposition="merge", primary_key="id")
def fetch_crime_data():
    # headers = {"X-App-Token": CHI_APP_TOKEN} if CHI_APP_TOKEN else {}
    limit = 1000
    offset = 0
    pages = 0
    while True:
        url = (
            "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
            f"?$where=year=2025&$limit={limit}&$offset={offset}&$order=id"
        )
        data = requests.get(url).json()
        if not data:
            break
        yield data
        offset += limit
        pages += 1
        if ONLY_FIRST_PAGE and pages >= 1:
            break

# Create the resource and apply hints
source_data = fetch_crime_data()
source_data.apply_hints(columns=HINTS)

# Initialize the pipeline (destination won't be used because we won't load)
pipeline = dlt.pipeline(
    pipeline_name="chicago_crime_pipeline",
    destination="bigquery",
    dataset_name="chicago_crime_data"
)

# ---- Extract ONLY (no load) and print YAML schema ----
pipeline.extract(source_data)  # no load
print(pipeline.default_schema.to_pretty_yaml())
