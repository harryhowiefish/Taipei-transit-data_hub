import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from google.cloud import storage
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv('../.env')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp_credentials/GCS_BigQuery_write_cred.json'


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
        self.access_token = response.json()['access_token']
        return self.access_token

    def get_response(self, url):
        headers = {'authorization': f'Bearer {self.get_token()}'}
        response = requests.get(url, headers=headers)
        return response.json()


def get_bus_data() -> pd.DataFrame:
    tdx = TDX(os.environ['TDX_APP_ID'], os.environ['TDX_APP_KEY'])

    data = tdx.get_response(
        "https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Station/TRTC?%24format=JSON")
    with open('taipei_bus_station_info.json', 'w') as f:
        json.dump(data, f, ensure_ascii=False)

    df = pd.json_normalize(data)
    return df


def upload_to_gcs(df, bucket_name, filename):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index=False))
    logging.info(f'file: {filename} created!')


def main():
    df = get_bus_data()
    bucket_name = 'static_reference'
    filename = 'mrt/mrt_station.csv'
    upload_to_gcs(df, bucket_name, filename)
    df.to_csv(filename, index=False)


if __name__ == '__main__':
    main()
