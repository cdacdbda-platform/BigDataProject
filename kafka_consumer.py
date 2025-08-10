import json
import time
from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime
import os

# OPTIMIZED CONFIGURATION FOR FAST PROCESSING
EXPECTED_RECORD_COUNT = 50000  # Expected total records
IDLE_TIMEOUT_SECONDS = 10      # Stop after 10 seconds of no messages
BATCH_FLUSH_SIZE = 1000        # Flush aggregated data every 1000 records

# Kafka setup with REDUCED timeout
bootstrap_servers = 'localhost:9092'
topics = [
    'synthetic_view_events',
    'synthetic_add_to_cart_events',
    'synthetic_purchase_events'
]

# Consumer setup with FAST timeout
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='clickstream-consumer-group',
    consumer_timeout_ms=3000,  # REDUCED from 30000 to 3000ms (3 seconds)
    fetch_min_bytes=1,         # Process messages immediately
    fetch_max_wait_ms=500,     # Don't wait long for batches
    max_poll_records=1000,     # Process more records per poll
    session_timeout_ms=10000,  # Reduced session timeout
    heartbeat_interval_ms=3000 # More frequent heartbeats
)

# Aggregation buffers
product_counts = defaultdict(int)
user_sessions = defaultdict(list)
daily_revenue = defaultdict(float)

# Output folder setup - use environment variable if available
output_dir = os.getenv('AGGREGATED_OUTPUT_DIR', 'aggregated_output')
os.makedirs(output_dir, exist_ok=True)

print("🎯 FAST Kafka consumer started. Listening to topics...")
print(f"⏱️  Will timeout after {IDLE_TIMEOUT_SECONDS} seconds of inactivity")
print(f"🎯 Expected records: {EXPECTED_RECORD_COUNT:,}")

processed_count = 0
last_message_time = time.time()
start_time = time.time()
consecutive_empty_polls = 0
MAX_EMPTY_POLLS = 5  # Stop after 5 consecutive empty polls

# Optimized file handles for batch writing
event_files = {}

try:
    print("🔄 Starting message consumption...")
    
    while True:
        # Poll for messages with timeout
        try:
            message_batch = consumer.poll(timeout_ms=1000)  # 1 second poll timeout
            
            if not message_batch:
                consecutive_empty_polls += 1
                current_time = time.time()
                idle_time = current_time - last_message_time
                
                print(f"⏳ No messages received (idle for {idle_time:.1f}s, empty polls: {consecutive_empty_polls})")
                
                # Stop if we've been idle too long OR too many empty polls
                if idle_time > IDLE_TIMEOUT_SECONDS or consecutive_empty_polls >= MAX_EMPTY_POLLS:
                    print(f"🛑 Stopping consumer: idle for {idle_time:.1f}s or {consecutive_empty_polls} empty polls")
                    break
                    
                continue
            
            # Reset counters when we get messages
            consecutive_empty_polls = 0
            
            # Process all messages in the batch
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    try:
                        event = message.value
                        event_type = event.get('event_type')
                        product_id = event.get('product_id')
                        user_id = event.get('user_id')
                        timestamp = event.get('timestamp')
                        
                        processed_count += 1
                        last_message_time = time.time()
                        
                        # Progress reporting every 5000 records
                        if processed_count % 5000 == 0:
                            elapsed = time.time() - start_time
                            rate = processed_count / elapsed
                            print(f"🔹 Processed {processed_count:,} events in {elapsed:.1f}s (rate: {rate:.1f}/sec)")
                        
                        # Aggregation: Product views/adds/purchases
                        key = (product_id, event_type)
                        product_counts[key] += 1
                        
                        # Session tracking (simplified for speed)
                        user_sessions[user_id].append((timestamp, event_type, product_id))
                        
                        # Revenue tracking (assuming each purchase is $50)
                        if event_type == 'purchase':
                            day = timestamp[:10]
                            daily_revenue[day] += 50.0
                        
                        # Batch write raw events (open files as needed)
                        if event_type not in event_files:
                            event_files[event_type] = open(
                                os.path.join(output_dir, f"{event_type}_events.jsonl"), "w"
                            )
                        
                        event_files[event_type].write(json.dumps(event) + "\n")
                        
                        # Flush files every batch
                        if processed_count % BATCH_FLUSH_SIZE == 0:
                            for f in event_files.values():
                                f.flush()
                        
                        # Early exit if we've processed expected number of records
                        if processed_count >= EXPECTED_RECORD_COUNT:
                            print(f"🎯 Reached expected record count: {EXPECTED_RECORD_COUNT:,}")
                            break
                            
                    except Exception as event_error:
                        print(f"Error processing individual event: {event_error}")
                        continue
                
                # Break out of outer loop too
                if processed_count >= EXPECTED_RECORD_COUNT:
                    break
            
            # Break out of main loop
            if processed_count >= EXPECTED_RECORD_COUNT:
                break
                
        except Exception as poll_error:
            if "timeout" in str(poll_error).lower():
                print(f"⏰ Consumer poll timeout after processing {processed_count} events")
                break
            else:
                print(f"Poll error: {str(poll_error)}")
                break

except KeyboardInterrupt:
    print(f"\n⚠️ Consumer interrupted by user after {processed_count} events")

except Exception as e:
    print(f"❌ Consumer error: {str(e)}")

finally:
    # Cleanup and final processing
    print(f"\n🔄 Finalizing consumer after processing {processed_count:,} events...")
    
    # Close all event files
    for event_type, file_handle in event_files.items():
        try:
            file_handle.flush()
            file_handle.close()
            print(f"✅ Closed {event_type}_events.jsonl")
        except Exception as e:
            print(f"⚠️ Error closing {event_type} file: {e}")
    
    # Save aggregated product counts
    try:
        product_counts_file = os.path.join(output_dir, "product_counts.csv")
        with open(product_counts_file, "w") as f:
            f.write("product_id,event_type,count\n")
            for (product_id, event_type), count in product_counts.items():
                f.write(f"{product_id},{event_type},{count}\n")
        print(f"✅ Product counts saved: {len(product_counts)} entries")
    except Exception as e:
        print(f"❌ Error saving product counts: {e}")
    
    # Save revenue summary
    try:
        revenue_file = os.path.join(output_dir, "daily_revenue.csv")
        with open(revenue_file, "w") as f:
            f.write("date,total_revenue\n")
            for date, revenue in sorted(daily_revenue.items()):
                f.write(f"{date},{revenue}\n")
        print(f"✅ Daily revenue saved: {len(daily_revenue)} days")
    except Exception as e:
        print(f"❌ Error saving daily revenue: {e}")
    
    # Close consumer connection
    try:
        consumer.close()
        print("✅ Consumer connection closed")
    except Exception as e:
        print(f"⚠️ Error closing consumer: {e}")
    
    # Final performance statistics
    end_time = time.time()
    total_time = end_time - start_time
    
    print("\n📊 FINAL PERFORMANCE REPORT:")
    print("=" * 50)
    print(f"   ⏱️  Total processing time: {total_time:.2f} seconds")
    print(f"   📊 Total events processed: {processed_count:,}")
    print(f"   📈 Processing rate: {processed_count/total_time:.1f} events/sec")
    print(f"   🎯 Target achievement: {(processed_count/EXPECTED_RECORD_COUNT)*100:.1f}%")
    print(f"   📁 Output directory: {output_dir}")
    
    # Verify output files
    output_files = [
        "product_counts.csv",
        "daily_revenue.csv", 
        "view_events.jsonl",
        "add_to_cart_events.jsonl",
        "purchase_events.jsonl"
    ]
    
    print(f"\n📁 Output files in {output_dir}:")
    for filename in output_files:
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            print(f"   ✅ {filename}: {size:,} bytes")
        else:
            print(f"   ❌ {filename}: Not found")
    
    if processed_count >= EXPECTED_RECORD_COUNT * 0.95:  # 95% success rate
        print("\n🎉 CONSUMER SUCCESS: Fast processing completed!")
    else:
        print(f"\n⚠️  CONSUMER WARNING: Only processed {processed_count:,}/{EXPECTED_RECORD_COUNT:,} records")
    
    print("=" * 50)