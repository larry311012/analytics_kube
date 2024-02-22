from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import sys
import boto3
from io import StringIO
from airflow.hooks.S3_hook import S3Hook

def extract(api_key):
    fields = "&_fields=flight_iata,dep_iata,dep_time_utc,arr_iata,arr_time_utc,status,duration,delayed,dep_delayed,arr_delayed"
    method = 'ping'
    params = {'api_key': api_key}
    schedules_api = 'https://airlabs.co/api/v9/schedules?airline_iata=FR'
    print("Extracting...")
    schedule_data = pd.json_normalize(requests.get(schedules_api+fields+method, params).json(), record_path=['response'])
    return schedule_data 

def save_to_s3(data, bucket_name, object_name):
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Check if the file already exists in S3
    # if s3_hook.check_for_key(object_name, bucket_name=bucket_name):
    #     # If exists, read the existing data into a DataFrame
    old_data = s3_hook.read_key(object_name, bucket_name=bucket_name)
    old_data_df = pd.read_csv(StringIO(old_data))
        # Concatenate new data with the old data
    all_data_df = pd.concat([old_data_df, data], ignore_index=True)
    # else:
    #     all_data_df = data
    
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    all_data_df.to_csv(csv_buffer, index=False)
    
    # Upload the concatenated CSV to S3
    s3_hook.load_string(csv_buffer.getvalue(), object_name, bucket_name=bucket_name, replace=True)
    print(f"Data appended to s3://{bucket_name}/{object_name}")

def main():
    api_key = '831739b7-722e-4af1-96db-8242aedc783f'
    if not api_key:
        print("API key not found. Set the AIRLABS_API_KEY environment variable.")
        sys.exit(1)

    # Define your S3 bucket and object name
    bucket_name = 'analytics-kube'
    object_name = 'schedule.csv'

    data = extract(api_key)
    save_to_s3(data, bucket_name, object_name)

    print("Data Uploaded to S3")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 19),
    'email': ['your_email@example.com'], 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('my_dag', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:
    python_task = PythonOperator(
        task_id ='my_python_task',
        python_callable = main
    )