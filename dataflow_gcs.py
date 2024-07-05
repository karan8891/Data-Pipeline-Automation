from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromPubSub, WriteToText
from apache_beam.transforms import Map, WindowInto
import apache_beam as beam
import json
from data_schema import data_schema

# Define your project ID, temporary location (optional), and staging location (optional)
project_id = "genial-upgrade-427514-r9"
temp_location = "gs://test-datapipeline/temp"  # Optional, replace with your GCS bucket path
staging_location = "gs://test-datapipeline/staging"  # Optional, replace with your GCS bucket path
region = "asia-south2"  # Replace with your desired region code

# Function to transform data (replace with your logic)
def transform_data(data):
  # Convert data from JSON string to dictionary
  data_dict = json.loads(data)

  # Perform basic transformation (e.g., select specific fields)
  transformed_data = {"name": data_dict["name"], "age": str(data_dict["age"])}

  # Return transformed data as a string
  return json.dumps(transformed_data)

# Build pipeline options
options = PipelineOptions(runner='DataflowRunner', project=project_id, temp_location=temp_location, staging_location=staging_location, region=region)

# Create the Dataflow pipeline
with beam.Pipeline(options=options) as pipeline:
  # Read data from Pub/Sub subscription (replace with your subscription name)
  subscription_name = "demo-topic-sub"
  data = pipeline | 'ReadFromPubSub' >> ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{subscription_name}")

    # Apply windowing (replace with your desired strategy)
  trigger = AfterWatermark()
  data = data | 'Window' >> beam.WindowInto(FixedWindows(60), trigger=trigger, accumulation_mode=AccumulationMode.DISCARDING)

  # Transform data (avoiding groupByKey)
  transformed_data = data | 'TransformData' >> Map(transform_data)

  # Write transformed data to GCS
  gcs_output_path = "gs://test-datapipeline/data/output.txt"
  transformed_data | 'WriteToGCS' >> WriteToText(gcs_output_path)

print(f"Dataflow pipeline successfully created to transform data and write to GCS: {gcs_output_path}")
