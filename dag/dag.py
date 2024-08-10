from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator, DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import json

# Define your variables
project_id = "genial-upgrade-427514-r9"
bucket_name = "your_bucket"
dataset_id = "your_dataset"
transaction_table_id = "transaction_data"
csv_source_path = f"gs://{bucket_name}/source_data.csv"
pubsub_topic = "projects/genial-upgrade-427514-r9/topics/your_topic"
api_endpoint = "https://api.example.com/data"
schema_file_path = "gs://your_bucket/schema.json"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'pubsub_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:

    # Task 1: Download CSV file from a source
    download_csv_task = GCSToGCSOperator(
        task_id='download_csv',
        source_bucket='source_bucket_name',
        source_object='path/to/your/source_file.csv',
        destination_bucket=bucket_name,
        destination_object='source_data.csv',
        move_object=False
    )

    # Task 2: Fetch data from API and publish to Pub/Sub
    def fetch_and_publish_api_data():
        response = requests.get(api_endpoint)
        data = response.json()
        message = json.dumps(data)
        
        pubsub_task = PubSubPublishMessageOperator(
            task_id='publish_to_pubsub',
            project=project_id,
            topic=pubsub_topic,
            messages=[{'data': message.encode('utf-8')}]
        )
        pubsub_task.execute(context={})

    fetch_api_data_task = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_and_publish_api_data
    )

    # Task 3: Trigger Dataflow job to process the data
    start_dataflow_task = DataflowCreatePythonJobOperator(
        task_id='start_dataflow_job',
        py_file='gs://your_bucket/path_to_your_dataflow_script.py',
        project_id=project_id,
        location='us-central1',
        job_name='dataflow-pipeline-job',
        options={
            'input_csv': csv_source_path,
            'output_table': f'{project_id}:{dataset_id}.{transaction_table_id}',
            'schema_file': schema_file_path,
        },
        py_interpreter='python3'
    )

    # Task 4: Load the data into BigQuery
    load_to_bq_task = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket=bucket_name,
        source_objects=['source_data.csv'],
        destination_project_dataset_table=f'{dataset_id}.{transaction_table_id}',
        schema_object='schema.json',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        allow_jagged_rows=True,
        field_delimiter=','
    )

    # Define the task dependencies
    download_csv_task >> fetch_api_data_task >> start_dataflow_task >> load_to_bq_task
