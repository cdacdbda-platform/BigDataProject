import random
import json
import time
import csv
import uuid
import os
import sys
import traceback
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

# Enhanced environment variable handling
csv_output_path = os.getenv('CSV_OUTPUT_PATH', 'synthetic_clickstream.csv')
print(f"📁 CSV will be saved to: {csv_output_path}")

# Ensure output directory exists
output_dir = os.path.dirname(csv_output_path)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")

# Sample dimensions
event_types = ['view', 'add_to_cart', 'purchase']
devices = ['mobile', 'desktop', 'tablet']
locations = ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai']
product_ids = [f"P{str(i).zfill(5)}" for i in range(1, 1001)]
user_ids = [f"U{str(i).zfill(4)}" for i in range(1, 501)]

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_names = [
    'synthetic_view_events',
    'synthetic_add_to_cart_events',
    'synthetic_purchase_events'
]

def cleanup_and_recreate_topics():
    """Clean up existing topics and create fresh ones"""
    print("🧹 Cleaning up and recreating Kafka topics...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, 
            client_id='clickstream-admin',
            request_timeout_ms=30000
        )
        
        # Get existing topics
        existing_topics = admin_client.list_topics()
        topics_to_delete = [topic for topic in topic_names if topic in existing_topics]
        
        # Delete existing topics
        if topics_to_delete:
            print(f"🗑️ Deleting existing topics: {topics_to_delete}")
            try:
                admin_client.delete_topics(topics_to_delete, timeout_ms=30000)
                print("✅ Topics deleted successfully")
                
                # Wait for deletion to complete
                print("⏱️ Waiting 5 seconds for topic deletion to complete...")
                time.sleep(5)
                
            except UnknownTopicOrPartitionError as e:
                print(f"⚠️ Some topics didn't exist: {e}")
            except Exception as e:
                print(f"⚠️ Error deleting topics: {e}")
        else:
            print("ℹ️ No existing topics to delete")
        
        # Create fresh topics
        print("🔧 Creating fresh topics...")
        new_topics = []
        for topic in topic_names:
            # Create topic with cleanup policy for faster deletion
            new_topic = NewTopic(
                name=topic,
                num_partitions=3,
                replication_factor=1,
                topic_configs={
                    'cleanup.policy': 'delete',
                    'retention.ms': '86400000',  # 1 day retention
                    'segment.ms': '3600000'      # 1 hour segments
                }
            )
            new_topics.append(new_topic)
        
        try:
            admin_client.create_topics(new_topics=new_topics, validate_only=False, timeout_ms=30000)
            print(f"✅ Created fresh topics: {[t.name for t in new_topics]}")
            
            # Wait for topic creation to complete
            time.sleep(3)
            
        except TopicAlreadyExistsError:
            print("⚠️ Some topics already exist after deletion - continuing anyway")
        
        admin_client.close()
        return True
        
    except Exception as e:
        print(f"❌ Failed to cleanup topics: {e}")
        print("Continuing with existing topics...")
        return False

# Cleanup topics before starting
cleanup_success = cleanup_and_recreate_topics()

# Enhanced Kafka producer setup with error handling
print("🔧 Setting up Kafka producers...")
producers = {}
kafka_available = True

try:
    # Wait a bit more if we just recreated topics
    if cleanup_success:
        print("⏱️ Waiting for topic initialization...")
        time.sleep(3)
    
    producers = {
        'view': KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all',
            max_block_ms=10000,  # Increased timeout
            batch_size=16384,
            linger_ms=10
        ),
        'add_to_cart': KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all',
            max_block_ms=10000,
            batch_size=16384,
            linger_ms=10
        ),
        'purchase': KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all',
            max_block_ms=10000,
            batch_size=16384,
            linger_ms=10
        )
    }
    print("✅ Kafka producers initialized successfully")
except Exception as e:
    print(f"⚠️ Kafka producer initialization failed: {e}")
    print("Will continue with CSV generation only")
    kafka_available = False
    producers = {}

# CSV file setup with error handling
print(f"📄 Setting up CSV file: {csv_output_path}")
try:
    csv_file = open(csv_output_path, 'w', newline='', encoding='utf-8')
    csv_writer = csv.DictWriter(csv_file, fieldnames=[
        'transaction_id', 'timestamp', 'user_id', 'event_type', 'product_id', 'device', 'location'])
    csv_writer.writeheader()
    csv_file.flush()
    print("✅ CSV file initialized successfully")
except Exception as e:
    print(f"❌ Failed to initialize CSV file: {e}")
    sys.exit(1)

def generate_event():
    """Generate a single event with error handling"""
    try:
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
    except Exception as e:
        print(f"Error generating event: {e}")
        return {
            "transaction_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": "U0001",
            "event_type": "view",
            "product_id": "P00001",
            "device": "desktop",
            "location": "Mumbai"
        }, "view"

# Enhanced record generation
print("🚀 Starting producer with clean topics...")
start_time = time.time()
successful_kafka_sends = 0
failed_kafka_sends = 0
csv_records_written = 0
total_records = 50000

try:
    for i in range(total_records):
        try:
            # Generate event
            event, topic = generate_event()
            
            # Always try to write to CSV first
            try:
                csv_writer.writerow(event)
                csv_records_written += 1
                
                # Flush CSV buffer every 1000 records
                if (i + 1) % 1000 == 0:
                    csv_file.flush()
                    
            except Exception as csv_error:
                print(f"CSV write error at record {i+1}: {csv_error}")
            
            # Try to send to Kafka if available
            if kafka_available and topic in producers:
                try:
                    future = producers[topic].send(f'synthetic_{topic}_events', value=event)
                    successful_kafka_sends += 1
                except Exception as kafka_error:
                    failed_kafka_sends += 1
                    if failed_kafka_sends <= 5:
                        print(f"Kafka error at record {i+1}: {kafka_error}")
                    elif failed_kafka_sends == 6:
                        print("Suppressing further Kafka error messages...")
            
            # Progress reporting every 10000 records
            if (i + 1) % 10000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                eta = (total_records - i - 1) / rate if rate > 0 else 0
                print(f"[{i+1:,}/{total_records:,}] Progress: {((i+1)/total_records)*100:.1f}% | "
                      f"CSV: {csv_records_written:,} | Kafka: {successful_kafka_sends:,} | "
                      f"Rate: {rate:.1f}/sec | ETA: {eta:.1f}sec")
                
            # Remove the time.sleep - let it run at full speed
            
        except KeyboardInterrupt:
            print(f"\n⚠️ Producer interrupted by user at record {i+1}")
            break
            
        except Exception as record_error:
            print(f"Unexpected error at record {i+1}: {record_error}")
            if i < 5:
                traceback.print_exc()
            continue
            
except Exception as fatal_error:
    print(f"❌ Fatal error in producer main loop: {fatal_error}")
    traceback.print_exc()

finally:
    # Cleanup and final reporting
    print("\n🔄 Cleaning up...")
    
    # Ensure CSV file is properly closed
    try:
        csv_file.flush()
        csv_file.close()
        print("✅ CSV file closed successfully")
    except Exception as e:
        print(f"⚠️ Error closing CSV file: {e}")
    
    # Flush all Kafka producers
    if kafka_available:
        for topic_name, producer in producers.items():
            try:
                print(f"Flushing {topic_name} producer...")
                producer.flush(timeout=30)
                producer.close()
            except Exception as e:
                print(f"⚠️ Error flushing {topic_name} producer: {e}")
    
    # Final statistics
    end_time = time.time()
    total_time = end_time - start_time
    
    print("\n📊 Final Statistics:")
    print(f"   ⏱️  Total runtime: {total_time:.2f} seconds")
    print(f"   📄 CSV records written: {csv_records_written:,}")
    print(f"   ✅ Kafka successful sends: {successful_kafka_sends:,}")
    print(f"   ❌ Kafka failed sends: {failed_kafka_sends:,}")
    print(f"   📈 Average rate: {csv_records_written/total_time:.1f} records/sec")
    
    # Verify CSV file
    try:
        if os.path.exists(csv_output_path):
            file_size = os.path.getsize(csv_output_path)
            print(f"   📁 CSV file size: {file_size:,} bytes")
            
            with open(csv_output_path, 'r') as f:
                line_count = sum(1 for line in f) - 1
            print(f"   📊 CSV actual record count: {line_count:,}")
            
            if line_count == total_records:
                print("✅ SUCCESS: All 50,000 records generated with clean topics!")
            else:
                print(f"⚠️ WARNING: Expected {total_records:,} but got {line_count:,} records")
        else:
            print(f"❌ ERROR: CSV file not found at {csv_output_path}")
    except Exception as e:
        print(f"⚠️ Error verifying CSV file: {e}")

print("✅ Producer execution completed with topic cleanup.")