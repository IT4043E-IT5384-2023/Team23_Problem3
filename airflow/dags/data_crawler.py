
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "..", ".."))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'tiennh',
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id='crawl_raw_data',
    default_args=default_args,
    description='Crawl raw data',
    start_date=datetime.fromtimestamp(1685433464),
    catchup=False,
    schedule_interval='10 8 * * *'
) as dag:

    pass