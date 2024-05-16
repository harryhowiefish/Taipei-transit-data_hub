from datetime import timedelta, datetime
from google.oauth2 import service_account
from airflow.decorators import dag, python_task
from google.cloud import storage
import requests
import pandas as pd
import logging
import pendulum
# import os
# import json


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

key_path = "/opt/airflow/dags/GCS_BigQuery_write_cred.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
CLIENT = storage.Client(credentials=credentials)


class TDX():
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self):
        token_url = 'https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token'
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        response = requests.post(token_url, headers=headers, data=data)
        # print(response.status_code)
        # print(response.json())
        self.access_token = response.json()['access_token']
        return self.access_token

    def get_response(self, url):
        headers = {'authorization': f'Bearer {self.get_token()}'}
        response = requests.get(url, headers=headers)
        return response.json()


CLIENT_ID = 'harryhowiefish-083228c0-a5d2-4c37'
CLIENT_SECRET = '971a0115-39a8-48b4-8122-ade8bca623d8'
tdx = TDX(CLIENT_ID, CLIENT_SECRET)


@dag(
    dag_id='bus_rt_to_gcs',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 4, 10),
    catchup=False)
def bus_rt_to_gcs():

    @python_task
    def get_rt_frequency(client, tdx):

        data = tdx.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeByFrequency/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        bucket = client.bucket('bus_realtime')
        filename = f'frequency/dt={now}.csv'
        blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index=False))
        logging.info(f'file: {filename} created!')

    @python_task
    def get_rt_nearstop(client, tdx):
        data = tdx.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeNearStop/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        bucket = client.bucket('bus_realtime')
        filename = f'nearstop/dt={now}.csv'
        blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index=False))
        logging.info(f'file: {filename} created!')

    @python_task
    def get_rt_estimate_arrival(client, tdx):
        data = tdx.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/EstimatedTimeOfArrival/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)
        now = pendulum.now('Asia/Taipei').format("MM_DD_YYYY/HH_mm")

        bucket = client.bucket('bus_realtime')
        filename = f'estimate_arrival/dt={now}.csv'
        blob = bucket.blob(filename)
        blob.upload_from_string(df.to_csv(index=False))
        logging.info(f'file: {filename} created!')

    get_rt_frequency(CLIENT, tdx)
    get_rt_nearstop(CLIENT, tdx)
    get_rt_estimate_arrival(CLIENT, tdx)
    # get_crowded_others(CLIENT)
    # get_crowded_BR(CLIENT)
    # get_rt_arrival(CLIENT)


bus_rt_to_gcs()
