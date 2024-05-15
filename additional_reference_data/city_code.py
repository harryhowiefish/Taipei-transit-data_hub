'''
upload hard coded city code mapping
'''
import pandas as pd
import os
from google.cloud import storage
import logging

logging.basicConfig(level=logging.INFO)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp_credentials/GCS_BigQuery_write_cred.json'


def city_code_dict_to_df(data: list) -> pd.DataFrame:

    column = ['city_code', 'city_name']
    df = pd.DataFrame(data, columns=column)
    return df


def upload_to_gcs(df, bucket_name, filename):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index=False))
    logging.info(f'file: {filename} created!')


def main():
    city_data = [
        ['TPE', '臺北市'],
        ['NWT', '新北市']
    ]
    df = city_code_dict_to_df(city_data)
    bucket_name = 'static_reference'
    filename = 'additional_references/city_code.csv'
    upload_to_gcs(df, bucket_name, filename)


main()
