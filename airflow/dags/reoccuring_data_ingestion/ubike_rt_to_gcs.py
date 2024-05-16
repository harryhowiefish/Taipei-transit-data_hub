from datetime import timedelta, datetime
from google.oauth2 import service_account
from airflow.decorators import dag, python_task
from google.cloud import storage
import requests
import pandas as pd
import logging
import pendulum


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    dag_id='ubike_rt_to_gcs',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 4, 10),
    catchup=False)
def ubike_rt_to_gcs():

    @python_task
    def get_rt_data():

        # change this to match where the google key is located
        key_path = "/opt/airflow/dags/GCS_BigQuery_write_cred.json"

        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = storage.Client(credentials=credentials)
        resp = requests.get(
            'https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json')  # noqa
        single_result = resp.json()
        single_result = pd.json_normalize(single_result)
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        # the bucket has to be first created in GCS before used
        bucket = client.bucket('youbike_realtime')
        blob = bucket.blob(f'{now}.csv')
        blob.upload_from_string(single_result.to_csv(index=False))
        logging.info(f'file: {now}.csv created!')

    get_rt_data()


ubike_rt_to_gcs()
