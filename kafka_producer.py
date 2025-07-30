import random
import json
import time
import csv
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Sample dimensions
event_types = ['view', 'add_to_cart', 'purchase']
devices = ['mobile', 'desktop', 'tablet']
locations = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai']
product_ids = [f"P{str(i).zfill(5)}" for i in range(1, 1001)]
user_ids = [f"U{str(i).zfill(4)}" for i in range(1, 501)]

# Kafka topic setup with AdminClient
bootstrap_servers = 'localhost:9092'
topic_names = [
    'synthetic_view_events',
    'synthetic_add_to_cart_events',
    'synthetic_purchase_events'
]

admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id='clickstream-admin')
existing_topics = admin_client.list_topics()

new_topics = []
for topic in topic_names:
    if topic not in existing_topics:
        new_topics.append(NewTopic(name=topic, num_partitions=3, replication_factor=1))

if new_topics:
    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print(f"✅ Created topics: {[t.name for t in new_topics]}")
    except TopicAlreadyExistsError:
        print("⚠️ Some topics already exist. Skipped creation.")
else:
    print("✅ All Kafka topics already exist.")

admin_client.close()

# Kafka producer setup
producers = {
    'view': KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    ),
    'add_to_cart': KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    ),
    'purchase': KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
}

# CSV file setup
csv_file = open('synthetic_clickstream.csv', 'w', newline='')
csv_writer = csv.DictWriter(csv_file, fieldnames=[
    'transaction_id', 'timestamp', 'user_id', 'event_type', 'product_id', 'device', 'location'])
csv_writer.writeheader()

def generate_event():
    event_type = random.choices(event_types, weights=[0.7, 0.2, 0.1])[0]
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": random.choice(user_ids),
        "event_type": event_type,
        "product_id": random.choice(product_ids),
        "device": random.choice(devices),
        "location": random.choice(locations)
    }, event_type

# Generate and send 50,000 records
for i in range(50000):
    event, topic = generate_event()
    producers[topic].send(f'synthetic_{topic}_events', value=event)
    csv_writer.writerow(event)
    print(f"[{i+1}] Sent to {topic} topic: {event}")
    time.sleep(0.01)  # simulate ~100 events/sec

csv_file.close()
for producer in producers.values():
    producer.flush()
print("✅ Finished sending and saving 50,000 records.")
