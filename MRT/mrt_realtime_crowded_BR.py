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


def E_mrt_crowded_BR():
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
    return (df)


def T_mrt_crowded_BR(df):
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
    df.to_csv(f"./{filename}mrt_realtime_crowded_BR.csv",
              encoding="utf-8-sig", index=False)
    return ("OK")
    # return(df)


data = E_mrt_crowded_BR()
T_mrt_crowded_BR(df=data)
