import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import re
from sqlalchemy import create_engine, exc
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from utils.gcp.update_fact_table import update_data_insert_merge_into_ods, update_data_insert_merge_into_fact_bike_realtime

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/harry_GCS_BigQuery_write_cred.json'
BQ_CLIENT_DOCKER = bigquery.Client()


def update_bike_realtime_ods():
    update_data_insert_merge_into_ods(target_dataset_name="ETL_ODS",
                                      source_dataset_name="ETL_SRC",
                                      source_table_name="SRC_youbike_after0504",
                                      target_table_name="ODS_youbike_realtime",
                                      client=BQ_CLIENT_DOCKER)
    print("task1 update_bike_realtime_ods finished")


def update_bike_realtime_fact():
    update_data_insert_merge_into_fact_bike_realtime(target_dataset_name="ETL_FACT",
                                                     source_dataset_name="ETL_ODS",
                                                     source_table_name="ODS_youbike_realtime",
                                                     target_table_name="FACT_bike_realtime",
                                                     client=BQ_CLIENT_DOCKER)
    print("task2 update_bike_realtime_fact finished")


default_args = {
    'owner': 'airflow',
    # If True, a task will depend on the success of the task in the previous run.
    'depends_on_past': False,
    # List of email addresses to send notifications.
    'email': ['lala9456@gmail.com'],
    'email_on_failure': False,  #
    'email_on_retry': False,
    'retries': 1,               # Number of retries if a task fails.
    # Time to wait before retrying a failed task.
    'retry_delay': timedelta(minutes=10),

}

dag = DAG(
    "DAG_youbike_realtim_update",
    default_args=default_args,
    description="insert new data into bike realtime ods and fact table",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    # If False, the scheduler won’t go back in time to run missed runs.
    catchup=False,
    tags=["bikerealtime", "update", "dim", "fact"]
)

task1_update_ods = PythonOperator(
    task_id='update_bike_realtime_ods',
    # Reference to a Python function to be executed by this task.
    python_callable=update_bike_realtime_ods,
    dag=dag,                        # The DAG to which the task belongs.
)

task2_update_fact = PythonOperator(
    task_id='update_bike_realtime_fact',
    # Reference to a Python function to be executed by this task.
    python_callable=update_bike_realtime_fact,
    dag=dag,                        # The DAG to which the task belongs.
)

task1_update_ods >> task2_update_fact
