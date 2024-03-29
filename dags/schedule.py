import pandas as pd
import requests
import os
import sys
import boto3
from io import StringIO 

def extract(api_key):
    fields = "&_fields=flight_iata,dep_iata,dep_time_utc,arr_iata,arr_time_utc,status,duration,delayed,dep_delayed,arr_delayed"
    schedules_api = 'https://airlabs.co/api/v9/schedules?airline_iata=FR'
    print("Extracting...")
    try:
        response = requests.get(f"{schedules_api}{fields}", params={'api_key': api_key})
        response.raise_for_status()  # This will raise an exception for HTTP error codes
        schedule_data = pd.json_normalize(response.json(), record_path=['response'])
        return schedule_data
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}", file=sys.stderr)
        sys.exit(1)

def save_to_s3(data, bucket_name, object_name):
    """
    Save the DataFrame to a CSV file and upload it to S3.
    :param data: DataFrame to save.
    :param bucket_name: S3 bucket name.
    :param object_name: S3 object name. Include the file name to save as in the bucket.
    """
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    # Create S3 client
    s3_client = boto3.client('s3')

    # Upload CSV to S3
    s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
    print(f"Data uploaded to s3://{bucket_name}/{object_name}")

def main():
    api_key = os.getenv("AIRLABS_API_KEY")
    if not api_key:
        print("API key not found. Set the AIRLABS_API_KEY environment variable.")
        sys.exit(1)

    # Define your S3 bucket and object name
    bucket_name = 'analytics-kube'
    object_name = 'schedule.csv'

    data = extract(api_key)
    save_to_s3(data, bucket_name, object_name)

if __name__ == "__main__":
    main()
