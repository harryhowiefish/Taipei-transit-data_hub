from datetime import timedelta, datetime
from airflow.decorators import dag, python_task
from google.cloud import storage
import requests
import pandas as pd
import logging
import pendulum
import os
import json
from utils.gcp import gcs

# these are some common arguments for dags
default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# get variables/infomation from enviornment (secrets or settings)
BUCKET_TYPE = os.environ['BUCKET_TYPE']
MRT_USER = os.environ['MRT_USERNAME']
MRT_PWD = os.environ['MRT_PASSWORD']


@dag(
    # basic setting for all dags
    dag_id='mrt_rt_to_gcs',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 4, 10),
    tags=["reoccuring", "data_ingestion"],
    catchup=False)
def mrt_rt_to_gcs():

    # setup the client that will be use in the dags
    gcs_client = storage.Client()

    @python_task
    def get_crowded_BL():

        # setup requirement for post requests
        url = "https://api.metro.taipei/metroapi/CarWeight.asmx"  # 板南線車廂擁擠度
        headers = {
            "Content-type": "text/xml; charset=utf-8"
        }
        xmldata = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
        <getCarWeightByInfo xmlns="http://tempuri.org/">
        <userName>{MRT_USER}</userName>
        <passWord>{MRT_PWD}</passWord>
        </getCarWeightByInfo>
        </soap:Body>
        </soap:Envelope>"""  # noqa

        # get data from MRT API
        response = requests.post(url=url, headers=headers, data=xmldata)
        json_df = json.loads(response.text.split("<?xml")[0])
        df = pd.DataFrame(json.loads(json_df))

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}mrt_realtime"
        filename = f'crowded/blue/dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(gcs_client, bucket_name, filename, df)
        if result:
            logging.info(f'file: {filename} created!')

    @python_task
    def get_crowded_others():

        # setup requirement for post requests
        url = "https://api.metro.taipei/metroapiex/CarWeight.asmx"  # 高運量車廂擁擠度
        headers = {
            "Content-type": "text/xml; charset=utf-8"
        }
        xmldata = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
        <getCarWeightByInfoEx xmlns="http://tempuri.org/">
        <userName>{MRT_USER}</userName>
        <passWord>{MRT_PWD}</passWord>
        </getCarWeightByInfoEx>
        </soap:Body>
        </soap:Envelope>"""  # noqa

        # get data from MRT API
        response = requests.post(url=url, headers=headers, data=xmldata)
        df = pd.json_normalize(json.loads(response.text.split("<?xml")[0]))

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}mrt_realtime"
        filename = f'crowded/others/dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(gcs_client, bucket_name, filename, df)
        if result:
            logging.info(f'file: {filename} created!')

    @python_task
    def get_crowded_BR():

        # setup requirement for post requests
        url = "https://api.metro.taipei/metroapi/CarWeightBR.asmx"  # 文湖線車廂擁擠度
        headers = {
            "Content-type": "text/xml; charset=utf-8"
        }
        xmldata = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
        <getCarWeightBRInfo xmlns="http://tempuri.org/">
        <userName>{MRT_USER}</userName>
        <passWord>{MRT_PWD}</passWord>
        </getCarWeightBRInfo>
        </soap:Body>
        </soap:Envelope>"""  # noqa

        # get data from MRT API
        response = requests.post(url=url, headers=headers, data=xmldata)
        df = pd.json_normalize(json.loads(response.text.split(
            "<getCarWeightBRInfoResult>")[
            1].split("</getCarWeightBRInfoResult")[0]))

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}mrt_realtime"
        filename = f'crowded/brown/dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(gcs_client, bucket_name, filename, df)
        if result:
            logging.info(f'file: {filename} created!')

    @python_task
    def get_rt_arrival():

        # setup requirement for post requests
        url = "https://api.metro.taipei/metroapi/TrackInfo.asmx"  # 列車到站資訊
        headers = {
            "Content-type": "text/xml; charset=utf-8"
        }
        xmldata = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
        <getTrackInfo xmlns="http://tempuri.org/">
        <userName>{MRT_USER}</userName>
        <passWord>{MRT_PWD}</passWord>
        </getTrackInfo>
        </soap:Body>
        </soap:Envelope>
        """  # noqa

        # get data from MRT API
        response = requests.post(url=url, headers=headers, data=xmldata)
        df = pd.json_normalize(json.loads(response.text.split("<?xml")[0]))

        # set up the required names for inserting the data into GCS
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")
        bucket_name = f"{BUCKET_TYPE}mrt_realtime"
        filename = f'arrival/dt={now}.csv'

        # inserting the file into GCS
        result = gcs.upload_df_to_gcs(gcs_client, bucket_name, filename, df)
        if result:
            logging.info(f'file: {filename} created!')

    # call the previously defined functions
    get_crowded_BL()
    get_crowded_others()
    get_crowded_BR()
    get_rt_arrival()


# this actually runs the whole DAG
mrt_rt_to_gcs()
