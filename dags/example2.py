from io import StringIO
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

def get_clickhouse_conn_details(conn_id='clickhouse'):
    # Fetch ClickHouse connection details from Airflow
    conn = BaseHook.get_connection(conn_id)
    return {
        'host': conn.host,
        'port': conn.port or '9000',
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema or 'default'
    }

def update_clickhouse_table():
    bucket_name = 'analytics-kube'
    object_name = 'schedule.csv'
    
    # Fetch the updated CSV from S3
    s3_hook = S3Hook(aws_conn_id='aws_default')
    csv_str = s3_hook.read_key(object_name, bucket_name)
    updated_df = pd.read_csv(StringIO(csv_str))
    
    # Connect to ClickHouse using Airflow connection details
    ch_conn_details = get_clickhouse_conn_details('clickhouse_default')
    clickhouse_client = Client(**ch_conn_details)
    
    # Prepare the data for insertion
    # Transform DataFrame to a list of tuples, which is the format expected by clickhouse_driver
    records = list(updated_df.itertuples(index=False, name=None))
    
    # Define the insertion query, adjusting columns as necessary to match your CSV structure and table schema
    query = 'INSERT INTO default.s3_schedule (column1, column2, ...) VALUES'
    
    # Execute the insertion
    clickhouse_client.execute(query, records)
    
    print("ClickHouse table updated with new rows from S3.")

def main():
    update_clickhouse_table()

# Assume `main` is being called appropriately within your Airflow DAG's PythonOperator
update_clickhouse_operator = PythonOperator(
    task_id='update_clickhouse',
    python_callable=update_clickhouse_table,
    dag=dag,
)

python_task >> update_clickhouse_operator