from google.cloud import pubsub_v1
import random
import json
import csv
import requests

# Define project ID and topic name (replace with yours)
project_id = "genial-upgrade-427514-r9"
topic_name = "demo-topic"

# Function to read data from a CSV file
def read_data_from_csv(file_path):
    with open(file_path, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]

# Function to fetch data from an API (replace with your API endpoint)
def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    return response.json()

# Function to create a Pub/Sub message from data source
def create_message(data):
    message_data = json.dumps(data)
    return message_data.encode('utf-8')

# Create a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Get the topic path
topic_path = publisher.topic_path(project_id, topic_name)

# Select data source (replace with your file path or API endpoint)
csv_file_path = "path_to_your_file.csv"
api_url = "https://api.example.com/data"

# Read data from the chosen source
data_source = "csv"  # or "api"
if data_source == "csv":
    data_list = read_data_from_csv(csv_file_path)
elif data_source == "api":
    data_list = fetch_data_from_api(api_url)

# Publish messages from the data source
for data in data_list:
    message = create_message(data)
    future = publisher.publish(topic=topic_path, data=message)
    print(f"Published message ID: {future.result()}")

print(f"Published {len(data_list)} messages to topic: {topic_name}")
