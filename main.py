import json
from typing import Any
import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import bigquery, storage

PROJECT_ID = "mindful-hull-450810-e9"
DATASET_ID = "finnhub"
TABLE_ID = "finnhub_table"

def insert_json_text_to_bq(bq_client: Any, text: str, file_name: str) -> None:
    try:
        json_data = json.loads(text)
        # BQ requires list of objects
        if isinstance(json_data, dict): json_data = [json_data]
        
        errors = bq_client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", json_data)
        
        if errors:
            print("BigQuery Insert Errors:", errors)
        else:
            print("Data successfully inserted into BigQuery.")
    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        
@functions_framework.cloud_event
def app(event: CloudEvent) -> None:
    """Triggered when a file is created in GCS. Reads JSON and loads into BigQuery."""
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)

    event_data = event.data
    bucket_name = event_data["bucket"]  
    file_name = event_data["name"]
    
    print(f"Processing file: gs://{bucket_name}/{file_name}")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    json_content = blob.download_as_text()
    
    insert_json_text_to_bq(bq_client, json_content, file_name)
    