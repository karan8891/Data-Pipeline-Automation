from google.cloud import bigquery
from google.cloud import storage
import json

# Initialize the BigQuery client
bigquery_client = bigquery.Client()

# Initialize the GCS client
storage_client = storage.Client()

# Define the GCS bucket and file
bucket_name = 'test-datapipeline'
file_name = 'output/transformed_data.json'
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Download the file contents as a string
data_string = blob.download_as_string().decode('utf-8')

# Parse the string as JSON
rows_to_insert = [json.loads(line) for line in data_string.splitlines()]

# Define the dataset and table
dataset_id = 'demo'
table_id = 'pubsub2bq'
table_ref = bigquery_client.dataset(dataset_id).table(table_id)

# Insert data into BigQuery
errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

# Check for errors
if errors:
    print(f"Errors occurred while inserting rows: {errors}")
else:
    print("Data successfully inserted into BigQuery")
