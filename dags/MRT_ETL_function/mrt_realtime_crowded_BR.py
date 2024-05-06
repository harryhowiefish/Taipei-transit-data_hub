import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import re
import numpy as np
from sqlalchemy import create_engine, exc
load_dotenv()


def E_mrt_crowded_BR():
    username = os.getenv("ANDY_USERNAME")
    password = os.getenv("ANDY_PASSWORD")
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
    </soap:Envelope>"""

    response = requests.post(url=url, headers=headers, data=xmldata)
    df = pd.DataFrame(json.loads(response.text.split("<getCarWeightBRInfoResult>")[
                      1].split("</getCarWeightBRInfoResult")[0]))
    print("E_mrt_crowded_BR finished")
    return (df)


def T_mrt_crowded_BR(df: pd.DataFrame):
    pattern = re.compile(r"[A-Za-z]+\s*")
    df["StationName"] = df["StationName"].str.replace(pattern, "", regex=True)
    pattern = re.compile(r"^[A-Z]+")

    def pattern_match_station_line_type(x):
        if pattern.findall(x):
            return (pattern.findall(x)[0])
        else:
            return ("")
    df["line_type"] = df["StationID"].apply(pattern_match_station_line_type)

    df["cart5"] = np.nan
    df["cart6"] = np.nan

    df = df.loc[:, ["StationID", "StationName", "line_type", "DU",
                    "Car1", "Car2", "Car3", "Car4", "cart5", "cart6", "UpdateTime"]]
    df.rename(columns={
        "StationID": "mrt_station_id",
        "StationName": "mrt_station_name",
        "DU": "direction",
        "Car1": "cart1",
        "Car2": "cart2",
        "Car3": "cart3",
        "Car4": "cart4",
        "UpdateTime": "update_time",

    }, inplace=True)

    filename = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
    # df.to_csv(f"./{filename}mrt_realtime_crowded_BR.csv",
    #           encoding="utf-8-sig", index=False)
    # return ("OK")
    print("T_mrt_crowded_BR finished")
    return (df)


# def L_mrt_crowded_BR(df: pd.DataFrame):
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
#     print("OK")
#     return ("OK")


def L_mrt_crowded_BR(df: pd.DataFrame, port: str = "docker"):
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

    print("L_mrt_crowded_BR finished")
    return ("L_mrt_crowded_BR finished")


if __name__ == "main":
    E_df = E_mrt_crowded_BR()
    T_df = T_mrt_crowded_BR(df=E_df)
    L_mrt_crowded_BR(df=T_df)
