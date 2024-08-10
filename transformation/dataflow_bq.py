import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json

# Define your project ID, BigQuery dataset, and table name
project_id = "genial-upgrade-427514-r9"
subscription_name = "your-subscription-name"  # Replace with your subscription name
dataset_id = "your_dataset"  # Replace with your dataset name
table_id = "your_table"  # Replace with your table name

# Define the BigQuery schema
table_schema = {
    "fields": [
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "age", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "city", "type": "STRING", "mode": "REQUIRED"}
    ]
}

def parse_pubsub_message(message):
    """Parses the Pub/Sub message and returns a dictionary."""
    message_data = message.decode('utf-8')
    return json.loads(message_data)

def transform_data(element):
    """Transform data if necessary. In this example, no transformation is needed."""
    # Here, you can add any transformation logic you need
    return element

def run():
    # Set up Pipeline options
    options = PipelineOptions()

    # Set up Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.job_name = "pubsub-to-bigquery"
    google_cloud_options.staging_location = "gs://your-bucket/staging"  # Replace with your GCS bucket
    google_cloud_options.temp_location = "gs://your-bucket/temp"  # Replace with your GCS bucket
    google_cloud_options.region = "us-central1"  # Change to your preferred region

    # Set DataflowRunner
    options.view_as(StandardOptions).runner = "DataflowRunner"

    # Set up Setup options
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p 
            # Read messages from Pub/Sub
            | "Read from PubSub" >> ReadFromPubSub(subscription="projects/{}/subscriptions/{}".format(project_id, subscription_name))
            
            # Parse the Pub/Sub messages
            | "Parse JSON messages" >> beam.Map(lambda message: parse_pubsub_message(message))
            
            # Apply any transformations if needed
            | "Transform data" >> beam.Map(lambda element: transform_data(element))
            
            # Write the data to BigQuery
            | "Write to BigQuery" >> WriteToBigQuery(
                table="{}.{}.{}".format(project_id, dataset_id, table_id),
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()
