# [TDX Documentation](https://tdx.transportdata.tw/webapi/File/Swagger/V3/2998e851-81d0-40f5-b26d-77e2f5ac4118)

from collections import Counter
import pandas as pd
import json
import requests
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
        "https://tdx.transportdata.tw/api/basic/v2/Bus/Station/City/Taipei?%24format=JSON")

    df = pd.json_normalize(data)
    return df


def upload_to_gcs(df, bucket_name, filename):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index=False))
    logging.info(f'file: {filename} created!')


def geocoding_to_json(df):
    '''this function still need cleanup'''

    API_KEY = os.environ['google_map_api_key']
    api_results = []
    for idx, row in df.iterrows():
        if idx <= 5300:
            continue
        lat = row['lat']
        lng = row['lng']
        reverse_geocoding_url = f'https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&key={API_KEY}&language=zh-TW'  # noqa
        try:
            response = requests.get(reverse_geocoding_url)
            data = response.json()
            data['row'] = idx
            api_results.append(data)

        except:  # noqa
            print('failed')
            print(row)

        if idx % 10 == 0:
            print(f'{idx} success')

        if idx % 100 == 0 and idx > 0:
            with open(f'{idx}_result.json', 'w') as f:
                json.dump(api_results, f, ensure_ascii=False)
            api_results = []

    def extract_data(data):
        locations = data['results']
        area_level_2_options = []
        area_level_3_options = []
        if not locations:
            return None, None
        for loc in locations:
            components = loc['address_components']
            for component in components:
                if 'administrative_area_level_2' in component.get('types', ''):
                    area_level_2_options.append(component['long_name'])
                elif 'administrative_area_level_3' in component.get('types', ''):
                    area_level_3_options.append(component['long_name'])
        return Counter(area_level_2_options).most_common(n=1)[0][0], Counter(area_level_3_options).most_common(n=1)[0][0]  # noqa

    files = list(range(100, 5304, 100)) + [5304]
    for file in files:
        with open(f'{file}_result.json', 'r') as f:
            api_results = json.load(f)
        for data in api_results:
            level_2, level_3 = extract_data(data)
            df.loc[data['row'], 'district'] = level_2
            df.loc[data['row'], 'subarea'] = level_3

    df.to_csv('tpe_bus_station_info_after_api_call.csv', index=False)


def main():
    df = get_bus_data()
    bucket_name = 'static_reference'
    filename = 'bus_cleanup.csv'
    upload_to_gcs(df, bucket_name, filename)


if __name__ == '__main__':
    main()
