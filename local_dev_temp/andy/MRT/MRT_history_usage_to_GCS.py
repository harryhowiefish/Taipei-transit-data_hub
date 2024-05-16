import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import re
from sqlalchemy import create_engine, exc
from zoneinfo import ZoneInfo
from io import StringIO
from google.cloud import storage

GCS_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_FILE_PATH
GCS_CLIENT = storage.Client()


# mrt_usage_history
# get csv download of every month's data
# each url can get one month's data
def E_mrt_usage_history_csvfilelist():
    url = "https://data.taipei/api/dataset/63f31c7e-7fc3-418b-bd82-b95158755b4d/resource/eb481f58-1238-4cff-8caa-fa7bb20cb4f4/download"
    response = requests.get(url=url)
    response_list = response.text.split("\r")

    col_name = response_list[0].split(",")
    url_df = pd.concat([pd.DataFrame([response_list[i].split(
        ",")[1:]], columns=col_name[1:]) for i in range(1, len(response_list))], axis=0)
    url_df.reset_index(drop=True, inplace=True)
    print("E_mrt_usage_history_csvfilelist finished")
    return (url_df)


def E_mrt_usage_history_one_month(url: str):
    response = requests.get(url=url)
    StringIO_df = StringIO(response.content.decode("utf-8-sig"))
    df = pd.read_csv(StringIO_df)
    # pattern = re.compile(r"[A-Za-z]+")
    # df["進站"] = df["進站"].str.replace(pattern, "", regex=True)
    # df["出站"] = df["出站"].str.replace(pattern, "", regex=True)
    df.rename(columns={
        "日期": "date",
        "時段": "hour",
        "進站": "mrt_station_name_enter",
        "出站": "mrt_station_name_exit",
        "人次": "visitors_num"
    }, inplace=True)
    print(f"T_mrt_usage_history finished")
    return (df)


def upload_to_bucket_string(df: pd.DataFrame, blob_name: str, bucket_name: str, storage_client: storage.Client):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    csv_string = df.to_csv(index=False, encoding="utf-8-sig")
    blob.upload_from_string(csv_string)
    print(blob)
    return blob


if __name__ == "__main__":
    url_df = E_mrt_usage_history_csvfilelist()
    for i in range(len(url_df)-1, 0, -1):
        try:
            month = url_df.loc[i, "年月"]
            url = url_df.loc[i, "URL"]
            print(f"{month}  is downloading")
            df = E_mrt_usage_history_one_month(url=url)
            print(f"{month} download finished")
            print(f"{month} is being uploaded to GCS")
            upload_to_bucket_string(
                df=df, bucket_name="mrt_history_usage", blob_name=f"{month}_mrt_history_usage.csv", storage_client=GCS_CLIENT)
            print(f"{month} has been sucessfully uploaded to GCS")
        except:
            continue
