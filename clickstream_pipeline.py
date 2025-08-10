from datetime import datetime, timedelta
import subprocess
import time
import os
import signal
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from kafka.admin import KafkaAdminClient
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'kafka_producer_consumer_pipeline',
    default_args=default_args,
    description='Kafka Producer and Consumer Pipeline using separate files',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['kafka', 'streaming', 'clickstream'],
)

# Configuration - Update these paths to match your file locations
PRODUCER_SCRIPT_PATH = '/home/sunbeam/BigDataProject/kafka_producer.py '# Update this path
CONSUMER_SCRIPT_PATH = '/home/sunbeam/BigDataProject/kafka_consumer.py'   # Update this path
OUTPUT_DIR = '/home/sunbeam/BigDataProject/output'  # Update this to your desired output directory
PYTHON_PATH = 'python'  # or 'python3' depending on your setup

def check_kafka_connection(**context):
    """Check if Kafka is running and accessible"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='localhost:9092',
            client_id='airflow-health-check'
        )
        topics = admin_client.list_topics()
        admin_client.close()
        logging.info(f"✅ Kafka is accessible. Found {len(topics)} topics.")
        return True
    except Exception as e:
        logging.error(f"❌ Kafka connection failed: {str(e)}")
        raise Exception(f"Kafka is not accessible: {str(e)}")

def check_script_files(**context):
    """Verify that producer.py and consumer.py files exist and create output directory"""
    missing_files = []
    
    if not os.path.exists(PRODUCER_SCRIPT_PATH):
        missing_files.append(f"Producer script: {PRODUCER_SCRIPT_PATH}")
    
    if not os.path.exists(CONSUMER_SCRIPT_PATH):
        missing_files.append(f"Consumer script: {CONSUMER_SCRIPT_PATH}")
    
    if missing_files:
        error_msg = f"Missing script files: {', '.join(missing_files)}"
        logging.error(f"❌ {error_msg}")
        raise FileNotFoundError(error_msg)
    
    # Create output directory if it doesn't exist
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        logging.info(f"📁 Output directory created/verified: {OUTPUT_DIR}")
    except Exception as e:
        logging.error(f"❌ Failed to create output directory {OUTPUT_DIR}: {str(e)}")
        raise
    
    logging.info("✅ Both producer.py and consumer.py files found.")
    return True

def start_producer(**context):
    """Start the Kafka producer using the separate producer.py file"""
    logging.info(f"🚀 Starting Kafka producer from {PRODUCER_SCRIPT_PATH}")
    
    try:
        # Set environment variables for output directory
        env = os.environ.copy()
        env['OUTPUT_DIR'] = OUTPUT_DIR
        env['CSV_OUTPUT_PATH'] = os.path.join(OUTPUT_DIR, 'synthetic_clickstream.csv')
        
        # Start the producer process
        process = subprocess.Popen(
            [PYTHON_PATH, PRODUCER_SCRIPT_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=os.path.dirname(PRODUCER_SCRIPT_PATH),
            env=env
        )
        
        # Store the process ID for monitoring
        with open('/tmp/producer_pid.txt', 'w') as f:
            f.write(str(process.pid))
        
        logging.info(f"✅ Producer started with PID: {process.pid}")
        logging.info(f"📁 CSV output will be saved to: {os.path.join(OUTPUT_DIR, 'synthetic_clickstream.csv')}")
        
        # Store process reference in XCom for other tasks
        context['task_instance'].xcom_push(key='producer_pid', value=process.pid)
        
        return process.pid
        
    except Exception as e:
        logging.error(f"❌ Failed to start producer: {str(e)}")
        raise

def start_consumer_after_delay(**context):
    """Start the consumer after a 5-second delay using the separate consumer.py file"""
    logging.info("⏱️ Waiting 5 seconds before starting consumer...")
    time.sleep(5)
    
    logging.info(f"🎯 Starting Kafka consumer from {CONSUMER_SCRIPT_PATH}")
    
    try:
        # Set environment variables for output directory
        env = os.environ.copy()
        env['OUTPUT_DIR'] = OUTPUT_DIR
        env['AGGREGATED_OUTPUT_DIR'] = os.path.join(OUTPUT_DIR, 'aggregated_output')
        
        # Start the consumer process
        process = subprocess.Popen(
            [PYTHON_PATH, CONSUMER_SCRIPT_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=os.path.dirname(CONSUMER_SCRIPT_PATH),
            env=env
        )
        
        # Store the process ID
        with open('/tmp/consumer_pid.txt', 'w') as f:
            f.write(str(process.pid))
        
        logging.info(f"✅ Consumer started with PID: {process.pid}")
        logging.info(f"📁 Aggregated output will be saved to: {os.path.join(OUTPUT_DIR, 'aggregated_output')}")
        
        # Monitor the consumer process
        max_wait_time = 3600  # 1 hour maximum wait time
        start_time = time.time()
        
        while process.poll() is None:  # While process is still running
            if time.time() - start_time > max_wait_time:
                logging.warning("⚠️ Consumer process exceeded maximum wait time. Terminating...")
                process.terminate()
                break
            
            time.sleep(10)  # Check every 10 seconds
        
        # Get the final output
        stdout, stderr = process.communicate()
        
        if stdout:
            logging.info(f"Consumer output: {stdout.decode()}")
        if stderr and process.returncode != 0:
            logging.error(f"Consumer errors: {stderr.decode()}")
        
        logging.info(f"✅ Consumer finished with return code: {process.returncode}")
        
        # Store completion info in XCom
        context['task_instance'].xcom_push(key='consumer_exit_code', value=process.returncode)
        
        return process.returncode
        
    except Exception as e:
        logging.error(f"❌ Failed to start/monitor consumer: {str(e)}")
        raise

def monitor_producer_and_cleanup(**context):
    """Monitor the producer process and perform cleanup"""
    logging.info("🔍 Starting producer monitoring and cleanup...")
    
    # Get producer PID from XCom
    producer_pid = context['task_instance'].xcom_pull(key='producer_pid', task_ids='start_producer')
    
    if not producer_pid:
        logging.warning("⚠️ Producer PID not found in XCom")
        return "No producer to monitor"
    
    try:
        # Monitor producer process
        max_wait_time = 3600  # 1 hour maximum
        start_time = time.time()
        producer_completed = False
        
        while time.time() - start_time < max_wait_time:
            try:
                # Check if process is still running (will raise exception if not)
                os.kill(producer_pid, 0)
                time.sleep(10)  # Check every 10 seconds
            except OSError:
                # Process has ended
                producer_completed = True
                logging.info("✅ Producer process has completed")
                break
        
        if not producer_completed:
            logging.warning("⚠️ Producer process exceeded maximum wait time")
            try:
                os.kill(producer_pid, signal.SIGTERM)
                logging.info("Terminated producer process")
            except OSError:
                pass  # Process might have already ended
    
    except Exception as e:
        logging.error(f"Error monitoring producer: {str(e)}")
    
    # Cleanup temporary files
    temp_files = [
        '/tmp/producer_pid.txt',
        '/tmp/consumer_pid.txt'
    ]
    
    for temp_file in temp_files:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                logging.info(f"Removed temporary file: {temp_file}")
        except Exception as e:
            logging.warning(f"Could not remove {temp_file}: {str(e)}")
    
    # Check for output files and log their locations
    output_locations = [
        os.path.join(OUTPUT_DIR, 'synthetic_clickstream.csv'),        # Producer output
        os.path.join(OUTPUT_DIR, 'aggregated_output'),                # Consumer output directory
        os.path.join(OUTPUT_DIR, 'aggregated_output', 'product_counts.csv'),
        os.path.join(OUTPUT_DIR, 'aggregated_output', 'daily_revenue.csv'),
    ]
    
    found_outputs = []
    for location in output_locations:
        if os.path.exists(location):
            if os.path.isfile(location):
                size = os.path.getsize(location)
                found_outputs.append(f"{location} ({size} bytes)")
            else:
                found_outputs.append(f"{location} (directory)")
    
    if found_outputs:
        logging.info(f"📁 Output files found:")
        for output in found_outputs:
            logging.info(f"   - {output}")
    else:
        logging.warning(f"⚠️ No output files found in {OUTPUT_DIR}")
    
    # Log the main output directory for easy reference
    logging.info(f"📂 All outputs should be in: {OUTPUT_DIR}")
    
    logging.info("🎉 Pipeline monitoring and cleanup completed!")
    return "Monitoring and cleanup completed successfully"

# Define the tasks
check_kafka_task = PythonOperator(
    task_id='check_kafka_connection',
    python_callable=check_kafka_connection,
    dag=dag,
)

check_files_task = PythonOperator(
    task_id='check_script_files',
    python_callable=check_script_files,
    dag=dag,
)

start_producer_task = PythonOperator(
    task_id='start_producer',
    python_callable=start_producer,
    dag=dag,
)

start_consumer_task = PythonOperator(
    task_id='start_consumer_after_delay',
    python_callable=start_consumer_after_delay,
    dag=dag,
)

monitor_cleanup_task = PythonOperator(
    task_id='monitor_producer_and_cleanup',
    python_callable=monitor_producer_and_cleanup,
    dag=dag,
)

# Set up task dependencies
# Both producer and consumer start in parallel after checks
check_kafka_task >> check_files_task >> [start_producer_task, start_consumer_task] >> monitor_cleanup_task