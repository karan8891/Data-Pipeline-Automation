from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Replace with your function names and arguments
from your_code import publish_messages, transform_data, load_to_bigquery

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 7, 6),
    "retries": 1,
}

with DAG(
    dag_id="pubsub_to_bigquery",
    default_args=default_args,
    schedule_interval="@hourly",  # Change for your desired schedule
) as dag:

    # Define tasks
    publish_task = PythonOperator(
        task_id="publish_messages",
        python_callable=publish_messages,
        op_args=[...],  # Arguments for publish_messages
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_args=[...],  # Arguments for transform_data
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        op_args=[...],  # Arguments for load_to_bigquery
    )

    # Define task dependencies
    publish_task >> transform_task >> load_task
