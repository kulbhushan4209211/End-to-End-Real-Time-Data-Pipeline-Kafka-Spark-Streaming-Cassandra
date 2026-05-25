from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator   
import json
import requests
from kafka import KafkaProducer
import time
import logging
import uuid

default_args = {
    'owner':'kulbhushan',
    'start_date':datetime(2026,5,21, 10,0,0)
}

def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    # res = json.dumps(res, indent=4)
    return res

def format_data(res):
    data= {}
    data['id'] = uuid.uuid4()
    data['name'] = res['name']['first']
    data['email'] = res['email']
    data['phone'] = res['phone']
    data['address'] = str(res['location']['street']['number']) + ',' + res['location']['street']['name'] + ',' + res['location']['city'] + ',' + res['location']['state']
    data['age'] = res['dob']['age']
    data['gender'] = res['gender']
    data['picture'] = res['picture']['large']
    data['username'] = res['login']['username']
    return data

def stream_data():
    res = get_data()
    data = format_data(res)

    producer = KafkaProducer(bootstrap_servers='broker1:29092',max_block_ms = 5000,value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'))
    curr_time = time.time()

    # 🟢 1. Initialize the start time checkpoint right before the loop
    curr_time = time.time()
    runtime_limit = 60  # 60 seconds duration

    logging.info(f"Starting streaming ingestion loop for {runtime_limit} seconds...")

    while True:
        # 🟢 2. Check if our 60-second execution window has expired
        if time.time() > (curr_time + runtime_limit):
            logging.info("Time limit reached. Exiting streaming loop gracefully...")
            break
            
        try:
            res = get_data()
            data = format_data(res)
            
            # Send data to Kafka broker
            producer.send('users', json.dumps(data, default=str).encode('utf-8'))
            
        except Exception as err:
            # Fixed logging comma syntax string formatting issue
            logging.error(f"Error occurred during streaming iteration: {err}")
            continue

    # 🟢 3. THE CRUCIAL AIRFLOW FIX: Flush buffers and close connection cleanly
    logging.info("Flushing remaining messages to Kafka broker...")
    producer.flush()  # Forces any buffered records to be sent immediately

    logging.info("Closing Kafka producer session...")
    producer.close()  # Safely disconnects from broker nodes

    logging.info("Task completed successfully!")

def register_schema():
    # Use the container name, not localhost!
    REGISTRY_URL = "http://schema-registry:8081" 
    SUBJECT_NAME = "users-value" # Assuming your topic is 'users'

    schema = {
        "type": "record",
        "name": "UserEvent",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "phone", "type": "string"},
            {"name": "address", "type": "string"},
            {"name": "age", "type": "string"},
            {"name": "gender", "type": "string"},
            {"name": "picture", "type": "string"},
            {"name": "username", "type": "string"}
        ]
    }
    
    response = requests.post(
        f"{REGISTRY_URL}/subjects/{SUBJECT_NAME}/versions",
        json={"schema": json.dumps(schema)},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
    )
    if response.status_code == 200:
        print(f"Successfully registered schema! ID: {response.json()['id']}")
    else:
        print(f"Failed to register: {response.text}")
with DAG('user-automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    # Task 1: Ensure schema exists
    setup_schema = PythonOperator(
        task_id='register_schema',
        python_callable=register_schema
    )
    
    # Task 2: Stream the data
    kafka_stream = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    # Task 3: 🟢 NEW TASK - Submits the batch processing job to Spark cluster
    # spark_transform_and_load = SSHOperator(
    #     task_id='submit_spark_to_cassandra',
    #     ssh_conn_id='ssh_spark_master',  # We will create this connection in Step 2
    #     command="spark-submit /opt/spark/apps/spark_stream.py"
    # )
    
    # Updated Task Pipeline Dependency Flow
    setup_schema >> kafka_stream
    

# if __name__ == "__main__":

#     data = stream_data()
#     print(data,'data')
