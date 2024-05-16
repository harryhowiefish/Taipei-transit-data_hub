import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import re
from sqlalchemy import create_engine, exc

import pymysql
from pymysql.err import IntegrityError, InternalError

load_dotenv()

app_id = os.getenv("TDX_APP_ID")
app_key = os.getenv("TDX_APP_KEY")


class Auth():

    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        content_type = 'application/x-www-form-urlencoded'
        grant_type = 'client_credentials'

        return {
            'content-type': content_type,
            'grant_type': grant_type,
            'client_id': self.app_id,
            'client_secret': self.app_key
        }


class data():

    def __init__(self, app_id, app_key, auth_response):
        self.app_id = app_id
        self.app_key = app_key
        self.auth_response = auth_response

    def get_data_header(self):
        if self.auth_response == "":
            access_token = ""
        else:
            auth_JSON = json.loads(self.auth_response.text)
            access_token = auth_JSON.get('access_token')

        return {
            'authorization': 'Bearer ' + access_token,
            'Accept-Encoding': 'gzip'
        }


def E_mrt_station():
    load_dotenv()

    app_id = os.getenv("TDX_APP_ID")
    app_key = os.getenv("TDX_APP_KEY")

    auth_url = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
    url = 'https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Station/TRTC?format=JSON'
    a = Auth(app_id, app_key)
    auth_response = requests.post(auth_url, a.get_auth_header())
    d = data(app_id, app_key, auth_response)
    response = requests.get(url, headers=d.get_data_header())
    return (pd.json_normalize(json.loads(response.text), max_level=1))


def T_mrt_station(df: pd.DataFrame):
    pattern = re.compile(r"^[A-Z]+")

    def pattern_match_station_line_type(x):
        if pattern.findall(x):
            return (pattern.findall(x)[0])
        else:
            return ("")
    df["line_type"] = df["StationID"].apply(pattern_match_station_line_type)
    df = df.loc[:, ['StationID', 'line_type', 'StationName.Zh_tw', 'StationName.En', 'StationAddress',
                    'StationPosition.PositionLat', 'StationPosition.PositionLon',
                    'LocationCityCode', 'LocationTown', 'BikeAllowOnHoliday', 'UpdateTime']]
    df.rename(columns={
        "StationID": "mrt_station_id",
        "StationName.Zh_tw": "mrt_station_name",
        "StationName.En": "station_en",
        "StationAddress": "station_address",
        "StationPosition.PositionLat": "lat",
        "StationPosition.PositionLon": "lon",
        "LocationCityCode": "city_code",
        "LocationTown": "district",
        "BikeAllowOnHoliday": "bike_allow_on_holiday",
        "UpdateTime": "update_time"
    }, inplace=True)
    return (df)

def L_mrt_station(df: pd.DataFrame):
    username_sql = os.getenv("ANDY_USERNAME_SQL")
    password_sql = os.getenv("ANDY_PASSWORD_SQL")
    # server = "host.docker.internal:3306"  #docker用
    server = "localhost:3306"
    db_name = "group2_db"
    with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:
            df.to_sql(
                name="mrt_station",
                con=conn,
                if_exists="replace",
                index=False
            )

    print("L_mrt_station finished")
    return ("L_mrt_station finished")

E_df = E_mrt_station()
T_df = T_mrt_station(df=E_df)
L_mrt_station(df=T_df)