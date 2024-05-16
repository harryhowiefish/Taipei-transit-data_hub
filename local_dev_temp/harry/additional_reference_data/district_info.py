'''
data source: https://www.post.gov.tw/post/download/1050812_行政區經緯度%28toPost%29.xml
'''

import requests
import pandas as pd
from io import StringIO
import os
from google.cloud import storage
import logging

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp_credentials/GCS_BigQuery_write_cred.json'


def xml_to_df_from_url(url: str) -> pd.DataFrame:
    resp = requests.get(
        'https://www.post.gov.tw/post/download/1050812_行政區經緯度%28toPost%29.xml')
    df = pd.read_xml(StringIO(resp.text))
    return df


def upload_to_gcs(df, bucket_name, filename):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index=False))
    logging.info(f'file: {filename} created!')


def main():
    path = 'https://www.post.gov.tw/post/download/1050812_行政區經緯度%28toPost%29.xml'
    df = xml_to_df_from_url(path)
    bucket_name = 'static_reference'
    filename = 'additional_references/district_info.csv'
    upload_to_gcs(df, bucket_name, filename)


main()
