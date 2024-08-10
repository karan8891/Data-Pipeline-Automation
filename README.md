
### Prerequisites

- Google Cloud Project
- Google Cloud SDK installed
- Access to Google Cloud services (BigQuery, Pub/Sub, Dataflow, Cloud Composer)
- Python 3.x installed

### Setup Instructions

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/yourusername/your-repo.git
   cd your-repo

---

# Data Ingestion and Processing Pipeline

This project implements a data ingestion and processing pipeline that retrieves data from various sources, processes it, and loads it into Google BigQuery. The pipeline is orchestrated using Google Cloud Composer (Airflow), and the data processing is performed using Google Dataflow. The components of the pipeline include:

- Ingesting data from a CSV file and an API.
- Using Google Pub/Sub for data ingestion.
- Transforming data using Google Dataflow.
- Performing lookups in BigQuery.
- Orchestrating the workflow with Cloud Composer.
- Automating deployments with Cloud Build.

## Project Overview

### Data Sources

1. **CSV File**: The application generates data in CSV format that is stored in a Google Cloud Storage (GCS) bucket.
2. **API**: The backend team provides additional data through a RESTful API.

### Ingestion Process

- **Pub/Sub**: The pipeline ingests data from both the CSV file and the API through Google Pub/Sub. The CSV data is published to a Pub/Sub topic, while the API data is fetched and published to the same topic.

### Data Processing

- **Dataflow**: The data processing is performed using Google Dataflow, which reads the data from Pub/Sub, applies transformations, and enriches it using lookups from BigQuery. The transformed data is then loaded into a BigQuery table.

### Lookups in BigQuery

- The pipeline performs lookups in BigQuery to enrich the data:
  - User details are fetched from the `user_lookup` table.
  - Product details are fetched from the `product_lookup` table.

### Orchestration

- **Cloud Composer**: The entire workflow is orchestrated using Google Cloud Composer, which runs Apache Airflow. Airflow DAGs manage the ingestion, processing, and loading of data.

### Automation with Cloud Build

- **Cloud Build**: The deployment process is automated using Google Cloud Build, which uploads the necessary files (DAGs, Dataflow scripts, schema files) to GCS and triggers the deployment of the Airflow DAG.

### Schema Files

- **Schema Definition**: The schema for BigQuery tables is defined in JSON files, which are utilized during the data loading process.

---

![Data Architecture](https://github.com/karan8891/Data-Pipeline-Automation/blob/main/images/datapipeline.jpg)

---

## Airflow Integration Explanation

This document explains how the provided code snippet can be used with Apache Airflow to orchestrate your data pipeline built in Project 1.

### Benefits of using Airflow

The code demonstrates how to integrate the Python functions you created in Project 1 with an Airflow DAG (Directed Acyclic Graph). Here's a breakdown of the advantages this integration offers:

* **Orchestration:** Airflow schedules and manages the execution order of tasks, automating the entire data pipeline.
* **Automation:** The DAG defines the task dependencies automating the entire data flow.
* **Reusability:** The code from project 1 (functions like `publish_messages` and `transform_data`) is reused within the Airflow operators, making your code modular.
* **Scheduling:** The `schedule_interval` parameter (in this example, @hourly) allows you to run the pipeline automatically on an hourly basis (adjust this as needed).
* **Monitoring:** Airflow provides a web interface to monitor the status of your DAG runs, track task successes/failures, and view logs.

Essentially, this code snippet leverages Airflow to transform your Python functions from project into a scheduled, automated data pipeline. Airflow takes care of the execution order, scheduling, and monitoring, making your data processing reliable and manageable.
