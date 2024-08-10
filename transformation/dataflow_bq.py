import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, SetupOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, ReadFromBigQuery
from apache_beam.pvalue import TaggedOutput
import json
import datetime

# Define your project ID, Pub/Sub subscription name, and BigQuery dataset/table names
project_id = "genial-upgrade-427514-r9"
subscription_name = "your_subscription_name"  # Replace with your subscription name
dataset_id = "your_dataset"  # Replace with your dataset name
transaction_table_id = "transaction_data"
error_table_id = "error_logs"
user_lookup_table = f"{project_id}.{dataset_id}.user_lookup"
product_lookup_table = f"{project_id}.{dataset_id}.product_lookup"

# Path to the schema file
schema_file_path = "schema.json"

def load_schema(schema_file):
    """Loads the BigQuery schema from a JSON file."""
    with open(schema_file, 'r') as file:
        schema = json.load(file)
    return schema

def parse_pubsub_message(message):
    """Parses the Pub/Sub message and returns a dictionary."""
    message_data = message.decode('utf-8')
    return json.loads(message_data)

def enrich_and_transform(element, user_lookup, product_lookup):
    """Enrich the data with user and product details, and apply business logic."""
    user_id = element.get('user_id')
    product_id = element.get('product_id')

    # Lookup user details
    user_info = user_lookup.get(user_id, None)
    if not user_info:
        return TaggedOutput('errors', {
            'error_message': f'User ID {user_id} not found in user lookup',
            'original_record': json.dumps(element),
            'error_time': datetime.datetime.now().isoformat()
        })
    
    # Lookup product details
    product_info = product_lookup.get(product_id, None)
    if not product_info:
        return TaggedOutput('errors', {
            'error_message': f'Product ID {product_id} not found in product lookup',
            'original_record': json.dumps(element),
            'error_time': datetime.datetime.now().isoformat()
        })

    # Enrich with user and product data
    element['user_name'] = user_info['user_name']
    element['user_email'] = user_info['user_email']
    element['product_name'] = product_info['product_name']
    element['product_category'] = product_info['product_category']

    # Apply some business logic (e.g., only consider transactions above a certain amount)
    if element['transaction_amount'] < 10:
        return TaggedOutput('errors', {
            'error_message': f'Transaction amount {element["transaction_amount"]} below threshold',
            'original_record': json.dumps(element),
            'error_time': datetime.datetime.now().isoformat()
        })
    
    # Return enriched and validated data
    return element

def run():
    # Set up Pipeline options
    options = PipelineOptions()

    # Set up Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.job_name = "complex-dataflow-pipeline"
    google_cloud_options.staging_location = "gs://your-bucket/staging"  # Replace with your GCS bucket
    google_cloud_options.temp_location = "gs://your-bucket/temp"  # Replace with your GCS bucket
    google_cloud_options.region = "us-central1"  # Change to your preferred region

    # Set DataflowRunner
    options.view_as(StandardOptions).runner = "DataflowRunner"

    # Set up Setup options
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True

    # Load the BigQuery schema from the schema.json file
    table_schema = load_schema(schema_file_path)

    with beam.Pipeline(options=options) as p:
        # Read the user lookup data from BigQuery
        user_lookup_data = (
            p
            | "Read User Lookup Table" >> ReadFromBigQuery(query=f'SELECT * FROM `{user_lookup_table}`', use_standard_sql=True)
            | "Convert User to Dict" >> beam.Map(lambda row: {row['user_id']: {"user_name": row['user_name'], "user_email": row['user_email']}})
            | "Merge User Dicts" >> beam.combiners.MergeAsDict()
        )

        # Read the product lookup data from BigQuery
        product_lookup_data = (
            p
            | "Read Product Lookup Table" >> ReadFromBigQuery(query=f'SELECT * FROM `{product_lookup_table}`', use_standard_sql=True)
            | "Convert Product to Dict" >> beam.Map(lambda row: {row['product_id']: {"product_name": row['product_name'], "product_category": row['product_category']}})
            | "Merge Product Dicts" >> beam.combiners.MergeAsDict()
        )

        # Read messages from Pub/Sub, enrich with lookup data, and process
        main_output, error_output = (
            p 
            | "Read from PubSub" >> ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{subscription_name}")
            | "Parse JSON messages" >> beam.Map(lambda message: parse_pubsub_message(message))
            | "Enrich and Transform Data" >> beam.Map(lambda element, users, products: enrich_and_transform(element, users, products),
                                                      beam.pvalue.AsSingleton(user_lookup_data),
                                                      beam.pvalue.AsSingleton(product_lookup_data)).with_outputs('errors', main='main')
        )

        # Write the main output to the transaction_data table
        main_output | "Write to BigQuery" >> WriteToBigQuery(
            table=f"{project_id}.{dataset_id}.{transaction_table_id}",
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

        # Write the errors to the error_logs table
        error_output | "Write Errors to BigQuery" >> WriteToBigQuery(
            table=f"{project_id}.{dataset_id}.{error_table_id}",
            schema={
                "fields": [
                    {"name": "error_message", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "original_record", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "error_time", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ]
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == "__main__":
    run()
