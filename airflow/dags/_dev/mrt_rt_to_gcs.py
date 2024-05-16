from datetime import timedelta, datetime
from google.oauth2 import service_account
from airflow.decorators import dag, python_task
from google.cloud import storage
import requests
import pandas as pd
import logging
import pendulum
# import os
import json


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

key_path = "/opt/airflow/dags/andy-gcs_key.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
CLIENT = storage.Client(credentials=credentials)


@dag(
    dag_id='mrt_rt_to_gcs',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 4, 10),
    catchup=False)
def mrt_rt_to_gcs():

    @python_task
    def get_crowded_BL(client):

        # change this to match where the google key is located

        username = 'harryhowiefish@gmail'
        password = 'aEePZKnk'
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
        <userName>{username}</userName>
        <passWord>{password}</passWord>
        </getCarWeightByInfo>
        </soap:Body>
        </soap:Envelope>"""  # noqa

        response = requests.post(url=url, headers=headers, data=xmldata)
        json_df = json.loads(response.text.split("<?xml")[0])
        df = pd.DataFrame(json.loads(json_df))
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        bucket = client.bucket('mrt_realtime')
        filename = f'crowded/blue/dt={now}.csv'
        blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index=False))
        logging.info(f'file: {filename} created!')

    @python_task
    def get_crowded_others(client):
        username = 'harryhowiefish@gmail'
        password = 'aEePZKnk'
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
        <userName>{username}</userName>
        <passWord>{password}</passWord>
        </getCarWeightByInfoEx>
        </soap:Body>
        </soap:Envelope>"""  # noqa

        response = requests.post(url=url, headers=headers, data=xmldata)
        df = pd.DataFrame(json.loads(response.text.split("<?xml")[0]))
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        bucket = client.bucket('mrt_realtime')
        filename = f'crowded/others/dt={now}.csv'
        blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index=False))
        logging.info(f'file: {filename} created!')

    @python_task
    def get_crowded_BR(client):
        username = 'harryhowiefish@gmail'
        password = 'aEePZKnk'
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
        <userName>{username}</userName>
        <passWord>{password}</passWord>
        </getCarWeightBRInfo>
        </soap:Body>
        </soap:Envelope>"""  # noqa

        response = requests.post(url=url, headers=headers, data=xmldata)
        df = pd.DataFrame(json.loads(response.text.split(
            "<getCarWeightBRInfoResult>")[
            1].split("</getCarWeightBRInfoResult")[0]))
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        bucket = client.bucket('mrt_realtime')
        filename = f'crowded/brown/dt={now}.csv'
        blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index=False))
        logging.info(f'file: {filename} created!')

    @python_task
    def get_rt_arrival(client):
        username = 'harryhowiefish@gmail'
        password = 'aEePZKnk'
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
        <userName>{username}</userName>
        <passWord>{password}</passWord>
        </getTrackInfo>
        </soap:Body>
        </soap:Envelope>
        """  # noqa

        response = requests.post(url=url, headers=headers, data=xmldata)
        df = pd.DataFrame(json.loads(response.text.split("<?xml")[0]))
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        bucket = client.bucket('mrt_realtime')
        filename = f'arrival/dt={now}.csv'
        blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index=False))
        logging.info(f'file: {filename} created!')

    get_crowded_BL(CLIENT)
    get_crowded_others(CLIENT)
    get_crowded_BR(CLIENT)
    get_rt_arrival(CLIENT)


mrt_rt_to_gcs()
