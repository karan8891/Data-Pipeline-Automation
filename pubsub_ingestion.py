from google.cloud import pubsub_v1
import random
import json
from data_schema import data_schema

# Define project ID and topic name (replace with yours)
project_id = "genial-upgrade-427514-r9"
topic_name = "demo-topic"

# Define your data schema (replace with your schema)
data_schema = {
  "name": "string",
  "age": "integer",
  "city": "string"
}

# Function to create a Pub/Sub message with schema and fake data
def create_message(data_schema):
  data = {}
  for field, field_type in data_schema.items():
    if field_type == "STRING":
      data[field] = f"fake_{field}_{random.randint(1, 100)}"
    elif field_type == "INTEGER":
      data[field] = random.randint(18, 65)
  message_data = json.dumps(data)
  return message_data.encode('utf-8')

# Create a Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Get the topic path
topic_path = publisher.topic_path(project_id, topic_name)

# Generate and publish 10 fake messages
num_messages = 10
for _ in range(num_messages):
  message = create_message(data_schema)
  future = publisher.publish(topic=topic_path, data=message)
  print(f"Published message ID: {future.result()}")

print(f"Published {num_messages} messages to topic: {topic_name}")
