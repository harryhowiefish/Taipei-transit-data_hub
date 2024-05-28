import pandas as pd
from google.cloud import storage
from airflow.decorators import dag, python_task
from utils.gcp import gcs
import os
import pendulum
import json
import requests

BUCKET_TYPE = os.environ['BUCKET_TYPE']
# BUCKET_TYPE = ''

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}

app_id = os.getenv("TDX_APP_ID")
app_key = os.getenv("TDX_APP_KEY")


class Auth():

    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        content_type = 'application/x-www-form-urlencoded'
        grant_type = 'client_credentials'

        return {
            'content-type': content_type,
            'grant_type': grant_type,
            'client_id': self.app_id,
            'client_secret': self.app_key
        }


class data():

    def __init__(self, app_id, app_key, auth_response):
        self.app_id = app_id
        self.app_key = app_key
        self.auth_response = auth_response

    def get_data_header(self):
        if self.auth_response == "":
            access_token = ""
        else:
            auth_JSON = json.loads(self.auth_response.text)
            access_token = auth_JSON.get('access_token')

        return {
            'authorization': 'Bearer ' + access_token,
            'Accept-Encoding': 'gzip'
        }


@dag(
    default_args=default_args,
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['mrt', 'one_time'],
)
def mrt_station_to_gcs():

    @python_task
    def download_to_df():

        auth_url = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"  # noqa
        url = 'https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Station/TRTC?format=JSON'  # noqa
        a = Auth(app_id, app_key)
        auth_response = requests.post(auth_url, a.get_auth_header())
        d = data(app_id, app_key, auth_response)
        response = requests.get(url, headers=d.get_data_header())
        return (pd.json_normalize(json.loads(response.text), max_level=1))

    @python_task
    def save_to_gcs(df) -> None:
        client = storage.Client()
        gcs.upload_df_to_gcs(client=client,
                             bucket_name=f'{BUCKET_TYPE}static_reference',
                             blob_name="mrt/trtc_station/trtc_station_00.csv",
                             df=df)
        return

    df = download_to_df()
    save_to_gcs(df)


mrt_station_to_gcs()
