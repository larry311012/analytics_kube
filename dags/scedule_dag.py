from airflow import DAG
from airflow.operators.python import PythonOperator  
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import sys
import boto3
from io import StringIO

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

def extract_and_save_to_s3(**kwargs):
    api_key = os.getenv("AIRLABS_API_KEY")
    if not api_key:
        print("API key not found. Set the AIRLABS_API_KEY environment variable.", file=sys.stderr)
        sys.exit(1)

    fields = "&_fields=flight_iata,dep_iata,dep_time_utc,arr_iata,arr_time_utc,status,duration,delayed,dep_delayed,arr_delayed"
    schedules_api = 'https://airlabs.co/api/v9/schedules?airline_iata=FR'
    print("Extracting...")
    try:
        response = requests.get(f"{schedules_api}{fields}", params={'api_key': api_key})
        response.raise_for_status() 
        schedule_data = pd.json_normalize(response.json(), record_path=['response'])
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}", file=sys.stderr)
        sys.exit(1)

    bucket_name = 'analytics-kube'
    object_name = 'schedule.csv'
    
    csv_buffer = StringIO()
    schedule_data.to_csv(csv_buffer, index=False)

    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        print(f"Data uploaded to s3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Failed to upload data to S3: {e}", file=sys.stderr)
        sys.exit(1)

with DAG(dag_id='air_dag', default_args=default_args, description='A simple DAG to run schedule.py with environment variables', schedule_interval=timedelta(minutes=3), catchup=False) as dag:
    run_etl = PythonOperator(
        task_id='extract_and_save_to_s3',
        python_callable=extract_and_save_to_s3,
        dag=dag,
    )
