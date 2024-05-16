import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import re
import numpy as np
from sqlalchemy import create_engine, exc
from zoneinfo import ZoneInfo
from google.cloud import storage
from MRT_ETL_function.upload_to_gcs_function import upload_to_bucket_string
load_dotenv()


def E_mrt_crowded_BL():
    username = os.getenv("ANDY_USERNAME")
    password = os.getenv("ANDY_PASSWORD")
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
    </soap:Envelope>"""

    response = requests.post(url=url, headers=headers, data=xmldata)
    json_df = json.loads(response.text.split("<?xml")[0])
    df = pd.DataFrame(json.loads(json_df))
    print("E_mrt_crowded_BL finished")
    return (df)


def T_mrt_crowded_BL(df: pd.DataFrame):
    pattern = re.compile(r"[A-Za-z]+\s*")
    df["StationName"] = df["StationName"].str.replace(pattern, "", regex=True)
    pattern = re.compile(r"^[A-Z]+")

    def pattern_match_station_line_type(x):
        if pattern.findall(x):
            return (pattern.findall(x)[0])
        else:
            return ("")
    df["line_type"] = df["StationID"].apply(pattern_match_station_line_type)

    df = df.loc[:, ["StationID", "StationName", "line_type", "CID",
                    "Car1", "Car2", "Car3", "Car4", "Car5", "Car6", "UpdateTime"]]
    df.rename(columns={
        "StationID": "mrt_station_id",
        "StationName": "mrt_station_name",
        "CID": "direction",
        "Car1": "cart1",
        "Car2": "cart2",
        "Car3": "cart3",
        "Car4": "cart4",
        "Car5": "cart5",
        "Car6": "cart6",
        "UpdateTime": "update_time",

    }, inplace=True)

    filename = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
    # df.to_csv(f"./{filename}mrt_realtime_crowded_BL.csv",
    #           encoding="utf-8-sig", index=False)
    # return ("OK")
    print("T_mrt_crowded_BL finished")
    return (df)


# def L_mrt_crowded_BL(df: pd.DataFrame):
#     username_sql = os.getenv("ANDY_USERNAME_SQL")
#     password_sql = os.getenv("ANDY_PASSWORD_SQL")
#     # server = "host.docker.internal:3306"  #docker用
#     server = "localhost:3306"
#     db_name = "group2_db"
#     with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:
#         df.to_sql(
#             name="mrt_realtime_crowded",
#             con=conn,
#             if_exists="append",
#             index=False
#         )
#     print("L_mrt_crowded_BL finished")
#     return ("OK")


def L_mrt_crowded_BL(df: pd.DataFrame, port: str = "docker"):
    username_sql = os.getenv("ANDY_USERNAME_SQL")
    password_sql = os.getenv("ANDY_PASSWORD_SQL")
    if port == "docker":
        server = "host.docker.internal:3306"  # docker用
    else:
        server = f"localhost:{port}"
    db_name = "group2_db"
    with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:
        df.reset_index(drop=True, inplace=True)
        for i in range(len(df)):
            row = df.loc[[i],]
            try:
                row.to_sql(
                    name="mrt_realtime_crowded",
                    con=conn,
                    if_exists="append",
                    index=False
                )
            except exc.IntegrityError:
                print("PK error :", end="")
                print(row)
                continue
            except Exception as e:
                print("Error!!!!")
                print(e)
                continue
    print("L_mrt_crowded_BL finished")
    return ("L_mrt_crowded_BL finished")

# def L_to_gcs_mrt_crowded_BL(df: pd.DataFrame, bucket_name: str):
#     now = datetime.strftime(datetime.now(
#         ZoneInfo('Asia/Taipei')), "%Y%m%d_%H%M")
#     blob_name = f"{bucket_name}{now}.csv"
#     upload_to_bucket_string(df=df, blob_name=blob_name,
#                             bucket_name=bucket_name)



if __name__ == "__main__":
    E_df = E_mrt_crowded_BL()
    T_df = T_mrt_crowded_BL(df=E_df)
    L_mrt_crowded_BL(df=T_df)
