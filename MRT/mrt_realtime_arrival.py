import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import re
import numpy as np
from sqlalchemy import create_engine
load_dotenv()
username = os.getenv("ANDY_USERNAME")
password = os.getenv("ANDY_PASSWORD")


def E_mrt_realtime_arrival():
    url = "https://api.metro.taipei/metroapi/TrackInfo.asmx"  # 列車到站資訊
    headers = {
        "Content-type": "text/xml; charset=utf-8"
    }
    xmldata = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
    <getTrackInfo xmlns="http://tempuri.org/">
    <userName>{username}</userName>
    <passWord>{password}</passWord>
    </getTrackInfo>
    </soap:Body>
    </soap:Envelope>
    """

    response = requests.post(url=url, headers=headers, data=xmldata)
    df = pd.DataFrame(json.loads(response.text.split("<?xml")[0]))
    print("E_mrt_realtime_arrival finished")
    return (df)


def T_mrt_realtime_arrival(df: pd.DataFrame):
    def arrive_time_transform(x):
        if x == "列車進站":
            return (0)
        elif x == "資料擷取中":
            return (np.nan)
        else:
            try:
                text_list = x.split(":")
                mins, seconds = int(text_list[0]), int(text_list[1])
                return (int(mins*60+seconds))
            except:
                return (np.nan)

    df["arrive_time"] = df["CountDown"].apply(arrive_time_transform)
    df = df.loc[:, ['StationName', 'DestinationName',
                    'arrive_time', 'NowDateTime']]
    df.rename(columns={
        "StationName": "mrt_station_name",
        "DestinationName": "mrt_destination_name",
        "NowDateTime": "update_time"
    }, inplace=True)
    df["mrt_station_name"] = df["mrt_station_name"].str.rstrip("站")
    df["mrt_destination_name"] = df["mrt_destination_name"].str.rstrip("站")
    filename = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
    # df.to_csv(f"./{filename}mrt_realtime_arrival.csv",
    #           encoding="utf-8-sig", index=False)
    # return ("OK")
    print("T_mrt_realtime_arrival finished")
    return (df)


def L_mrt_realtime_arrival(df: pd.DataFrame):
    username_sql = os.getenv("ANDY_USERNAME_SQL")
    password_sql = os.getenv("ANDY_PASSWORD_SQL")
    # server = "host.docker.internal:3306"  #docker用
    server = "localhost:3306"
    db_name = "group2_db"
    with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:
        df.to_sql(
            name="mrt_realtime_arrival",
            con=conn,
            if_exists="append",
            index=False
        )
    print("L_mrt_realtime_arrival finished")
    return ("L_mrt_realtime_arrival finished")


E_df = E_mrt_realtime_arrival()
T_df = T_mrt_realtime_arrival(df=E_df)
L_mrt_realtime_arrival(df=T_df)
