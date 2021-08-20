import os
import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'Data_Pipeline',
    default_args=args,
    description='Run this',
    schedule_interval="0 1 * * *",
    start_date=days_ago(1),
    tags=['Git API', 'Run this'],
) as dag:

    task1 = BashOperator(
        task_id='step 1',
        bash_command='python /opt/airflow/scripts/run.py'
    )

    task1