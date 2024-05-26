from datetime import timedelta, datetime
from airflow.decorators import dag, python_task
from google.cloud import storage
import pandas as pd
import logging
import pendulum
import os
from utils.gcp import gcs
from utils.etl import tdx

# these are some common arguments for dags
default_args = {
    'owner': 'TIR101_G2',
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

# get variables/infomation from enviornment (secrets or settings)
BUCKET_TYPE = os.environ['BUCKET_TYPE']
CLIENT_ID = os.environ['TDX_APP_ID']
CLIENT_SECRET = os.environ['TDX_APP_KEY']


@dag(
    # basic setting for all dags
    dag_id='bus_rt_to_gcs',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  # run every 10 minutes
    start_date=datetime(2024, 4, 10),
    tags=["reoccuring", "data_ingestion"],
    catchup=False)
def bus_rt_to_gcs():

    # setup the clients that will be use in the dags
    gcs_client = storage.Client()
    tdx_client = tdx.TDX(CLIENT_ID, CLIENT_SECRET)

    @python_task
    def get_rt_frequency():
        # get data from tdx's API
        data = tdx_client.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeByFrequency/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}bus_realtime"
        filename = f'frequency/dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(gcs_client, bucket_name, filename, df)
        if result:
            logging.info(f'file: {filename} created!')

    @python_task
    def get_rt_nearstop():
        # get data from tdx's API
        data = tdx_client.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeNearStop/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}bus_realtime"
        filename = f'nearstop/dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(gcs_client, bucket_name, filename, df)
        if result:
            logging.info(f'file: {filename} created!')

    @ python_task
    def get_rt_estimate_arrival():

        # get data from tdx's API
        data = tdx_client.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/EstimatedTimeOfArrival/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}bus_realtime"
        filename = f'estimate_arrival/dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(gcs_client, bucket_name, filename, df)
        if result:
            logging.info(f'file: {filename} created!')

    # call the previously defined functions
    get_rt_frequency()
    get_rt_nearstop()
    get_rt_estimate_arrival()


# this actually runs the whole DAG
bus_rt_to_gcs()
