import pendulum
import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, exc
from airflow.decorators import dag, task
from zoneinfo import ZoneInfo
from utils.etl.mrt_realtime_crowded_BL import E_mrt_crowded_BL, T_mrt_crowded_BL, L_mrt_crowded_BL
from utils.discord_notify_function import notify_failure, notify_success, dag_success_alert, task_failure_alert
from utils.gcp.gcs import upload_df_to_gcs
from google.cloud import storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/gcp_credentials/andy-gcs_key.json'
load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    'on_success_callback': notify_success,
    'on_failure_callback': notify_failure
}


@dag(
    dag_id="DAG_mrt_realtime_crowded_BL_to_gcs",
    default_args=default_args,
    description="ETL MRT realtime_crowded(BL line) data to gcs",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_success_callback=dag_success_alert,  # 在 DAG 成功時調用
    on_failure_callback=task_failure_alert,   # 在 DAG 失敗時調用
    # Optional: Add tags for better filtering in the UI
    tags=["BL", "MRT"]
)
def DAG_mrt_crowded_BL():
    @task
    def DAG_E_task():
        return (E_mrt_crowded_BL())

    @task
    def DAG_T_task(df):
        return (T_mrt_crowded_BL(df=df))

    @task
    def DAG_L_task(df, port):
        return (L_mrt_crowded_BL(df, port))

    @task
    def DAG_L_df_to_gcs_task(df):
        client = storage.Client()
        bucket_name = "testbucket0204"
        now = pendulum.now('Asia/Taipei').format("YYYY_MM_DD/HH_mm")
        blob_name = f"dt={now}_BL"
        result = upload_df_to_gcs(
            client=client, bucket_name=bucket_name, blob_name=blob_name, df=df)
        if result:
            logging.info(f'file: {blob_name} created!')
    E_df = DAG_E_task()
    T_df = DAG_T_task(df=E_df)
    DAG_L_task(df=T_df, port="docker")
    DAG_L_df_to_gcs_task(df=T_df)


DAG_mrt_crowded_BL()
