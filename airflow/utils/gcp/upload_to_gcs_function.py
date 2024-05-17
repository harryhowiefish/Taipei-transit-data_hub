from google.cloud import storage
import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo


def upload_to_bucket_string(df: pd.DataFrame, blob_name: str, bucket_name: str):
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\data_engineer\TIR_group2\TIR101_Group2\secrets\andy-gcs_key.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/andy-gcs_key.json'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_string = df.to_csv(index=False, encoding="utf-8-sig")
    blob.upload_from_string(csv_string)
    print(blob)
    return blob


def L_df_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name_tag: str = ""):
    now = datetime.strftime(datetime.now(
        ZoneInfo('Asia/Taipei')), "%Y%m%d_%H%M")
    blob_name = f"{bucket_name}{blob_name_tag}{now}.csv"
    upload_to_bucket_string(df=df, blob_name=blob_name,
                            bucket_name=bucket_name)
