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
from MRT_ETL_function.mrt_realtime_crowded_BL import E_mrt_crowded_BL, T_mrt_crowded_BL, L_mrt_crowded_BL
from MRT_ETL_function.upload_to_gcs_function import upload_to_bucket_string, L_df_to_gcs
# 使用getenv拿取帳號密碼
load_dotenv()

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
    dag_id="DAG_mrt_realtime_crowded_BL",
    default_args=default_args,
    description="ETL MRT realtime_crowded(BL line) data to mysql",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["BL"]
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
    def DAG_L_df_to_gcs_task(df, bucket_name, blob_name_tag):
        L_df_to_gcs(df=df, bucket_name=bucket_name,
                    blob_name_tag=blob_name_tag)

    E_df = DAG_E_task()
    T_df = DAG_T_task(df=E_df)
    DAG_L_task(df=T_df, port="docker")
    DAG_L_df_to_gcs_task(
        df=T_df, bucket_name="mrt_realtime_crowded", blob_name_tag="_BL")


DAG_mrt_crowded_BL()
