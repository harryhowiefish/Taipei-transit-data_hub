from datetime import timedelta, datetime
from airflow.decorators import dag, python_task
from google.cloud import storage
import requests
import pandas as pd
import logging
import pendulum
from utils.gcp import gcs
import os
from utils.discord import Simple_DC_Notifier

# get variables/infomation from enviornment (secrets or settings)
BUCKET_TYPE = os.environ['BUCKET_TYPE']


# these are some common arguments for dags
default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


@dag(
    # basic setting for all dags
    dag_id='ubike_rt_to_gcs',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 4, 10),
    on_success_callback=Simple_DC_Notifier('✅ success!'),
    tags=["reoccuring", "data_ingestion"],
    catchup=False)
def ubike_rt_to_gcs():
    # setup the client that will be use in the dags
    gcs_client = storage.Client()

    @python_task
    def get_rt_data():
        # get data from url
        resp = requests.get(
            'https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json')  # noqa
        single_result = resp.json()
        single_result = pd.json_normalize(single_result)

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}youbike_realtime"
        filename = f'dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(
            gcs_client, bucket_name, filename, single_result)
        if result:
            logging.info(f'file: {filename} created!')

    # call the previously defined functions
    get_rt_data()


# this actually runs the whole DAG
ubike_rt_to_gcs()
