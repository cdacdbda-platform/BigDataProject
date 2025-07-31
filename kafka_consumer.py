import json
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime
import os

# Kafka setup
bootstrap_servers = 'localhost:9092'
topics = [
    'synthetic_view_events',
    'synthetic_add_to_cart_events',
    'synthetic_purchase_events'
]

# Consumer setup
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='clickstream-consumer-group'
)

# Aggregation buffers
product_counts = defaultdict(int)
user_sessions = defaultdict(list)
daily_revenue = defaultdict(float)

# Output folder setup
output_dir = 'aggregated_output'
os.makedirs(output_dir, exist_ok=True)

print("✅ Kafka consumer started. Listening to topics...")

try:
    for message in consumer:
        event = message.value
        event_type = event.get('event_type')
        product_id = event.get('product_id')
        user_id = event.get('user_id')
        timestamp = event.get('timestamp')
        txn_id = event.get('transaction_id')
        device = event.get('device')
        location = event.get('location')

        print(f"🔹 Received {event_type} event: {txn_id}")

        # Aggregation: Product views/adds/purchases
        key = (product_id, event_type)
        product_counts[key] += 1

        # Session tracking (naive)
        user_sessions[user_id].append((timestamp, event_type, product_id))

        # Revenue tracking (assuming each purchase is $50)
        if event_type == 'purchase':
            day = timestamp[:10]
            daily_revenue[day] += 50.0  # replace with actual price if available

        # Optional: store raw events for Spark ingestion
        with open(os.path.join(output_dir, f"{event_type}_events.jsonl"), "a") as f:
            f.write(json.dumps(event) + "\n")

except KeyboardInterrupt:
    print("⛔ Stopping consumer and writing aggregates...")

finally:
    # Save aggregated product counts
    with open(os.path.join(output_dir, "product_counts.csv"), "w") as f:
        f.write("product_id,event_type,count\n")
        for (product_id, event_type), count in product_counts.items():
            f.write(f"{product_id},{event_type},{count}\n")

    # Save revenue summary
    with open(os.path.join(output_dir, "daily_revenue.csv"), "w") as f:
        f.write("date,total_revenue\n")
        for date, revenue in sorted(daily_revenue.items()):
            f.write(f"{date},{revenue}\n")

    print("✅ Aggregates written. Consumer shutdown complete.")
