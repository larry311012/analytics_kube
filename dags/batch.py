from airflow import DAG
from airflow.operators.bash import BashOperator  
from datetime import datetime, timedelta

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

dag = DAG(
    dag_id='air_dag',
    default_args=default_args,
    description='A simple DAG to run schedule.py with environment variables',
    schedule_interval=timedelta(minutes=3),
    catchup=False,  # Add this to prevent backfilling if your start_date is in the past
)

t1 = BashOperator(
    task_id='run_schedule_py',
    bash_command='python3 /opt/airflow/dags/schedule.py',
    env={'AIRLABS_API_KEY': '{{ var.value.AIRLABS_API_KEY }}'},  # Example of using Airflow Variables
    dag=dag,
)

t1