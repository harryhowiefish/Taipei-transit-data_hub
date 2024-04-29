import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import re
import numpy as np

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
    return (df)


def T_mrt_realtime_arrival(df):
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
    df.to_csv(f"./{filename}mrt_realtime_arrival.csv",
              encoding="utf-8-sig", index=False)
    return ("OK")
    # return(df)


data = E_mrt_realtime_arrival()
T_mrt_realtime_arrival(df=data)
