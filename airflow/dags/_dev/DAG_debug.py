import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import re
from sqlalchemy import create_engine, exc
from airflow.decorators import dag, task
from zoneinfo import ZoneInfo


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["lala9456@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="DAGtest",
    default_args=default_args,
    description="DAGtest",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["DAGtest"]
)
def DAGtest():
    @task
    def test1():
        print("dir is in")
        print(os.getcwd())
        print(os.listdir('/opt/airflow/dags'))
    test1()


DAGtest()
