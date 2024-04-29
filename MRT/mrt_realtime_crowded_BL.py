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


def E_mrt_crowded_BL():
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
    return (df)


def T_mrt_crowded_BL(df):
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
    df.to_csv(f"./{filename}mrt_realtime_crowded_BL.csv",
              encoding="utf-8-sig", index=False)
    return ("OK")
    # return(df)


data = E_mrt_crowded_BL()
T_mrt_crowded_BL(df=data)