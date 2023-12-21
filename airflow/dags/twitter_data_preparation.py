
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", ".."))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'binh.truong',
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='crawl_raw_data',
    default_args=default_args,
    description=(
        'Components: '
        '- Crawler: Get the raw data from Twitter, partitioned by topics.\n'
        '- Processor: Using pyspark to preprocess the data.\n'
        '- Directory listener: listen to new file from raw data directory to '
    start_date=datetime.fromtimestamp(1685433464),
    catchup=False,
    schedule_interval='10 8 * * *'
) as dag:

    pass