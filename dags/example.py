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

def my_callable(*args, **kwargs):
    print("Hello from PythonOperator")

with DAG('my_dag', default_args=default_args, start_date=datetime(2021, 1, 1)) as dag:
    python_task = PythonOperator(
        task_id='my_python_task',
        python_callable=my_callable
    )