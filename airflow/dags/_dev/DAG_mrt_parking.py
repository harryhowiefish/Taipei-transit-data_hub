import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime ,timedelta
import re
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from zoneinfo import ZoneInfo
# 使用getenv拿取帳號密碼
load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["lala9456@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="DAG_mrt_parking",
    default_args=default_args,
    description="ETL MRT parking data to mysql",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "decorator"]  # Optional: Add tags for better filtering in the UI
)

def DAG_mrt_parking():
    @task
    def E_mrt_parking():
        username = os.getenv("ANDY_USERNAME")
        password = os.getenv("ANDY_PASSWORD")
        url = "https://api.metro.taipei/MetroAPI/ParkingLot.asmx"
        headers = {
            "Content-type": "text/xml;charset=utf-8"
        }
        xmldata = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
        <getParkingLot xmlns="http://tempuri.org/">
        <userName>{username}</userName>
        <passWord>{password}</passWord>
        </getParkingLot>
        </soap:Body>
        </soap:Envelope>"""

        now = datetime.strftime(datetime.now(ZoneInfo('Asia/Taipei')), "%Y-%m-%d %H:%M:%S")
        response = requests.post(url=url, headers=headers,
                                data=xmldata.encode("utf-8"))
        df = pd.DataFrame(json.loads(response.text.split("<?xml")[0]))
        df["GetDatatime"] = now
        return (df)
    @task

    def T_mrt_parking(df):
        pattern = re.compile(r"[A-Z]+[0-9]+[A-Z]*")

        def pattern_match_station_id(x):
            if pattern.findall(x):
                return (pattern.findall(x)[0])
            else:
                return ("")

        def pattern_match_station_name(x):
            remove_id = pattern.split(x)[-1]
            return (remove_id.split("(")[0])

        df["StationNo"] = df["StationName"].apply(pattern_match_station_id)
        df["StationName"] = df["StationName"].apply(pattern_match_station_name)

        pattern = re.compile(r"^[A-Z]+")

        def pattern_match_station_line_type(x):
            if pattern.findall(x):
                return (pattern.findall(x)[0])
            else:
                return ("")
        df["line_type"] = df["StationNo"].apply(pattern_match_station_line_type)

        df = df.loc[:, ["ParkName", "StationNo", "StationName", "line_type",
                        "ParkType", "ParkNowNo", "ParkTotalNo", "GetDatatime",]]
        df.rename(columns={
            "ParkName": "park_name",
            "StationNo": "mrt_station_id",
            "StationName": "mrt_station_name",
            "line_type": "line_type",
            "ParkType": "parking_type",
            "ParkNowNo": "available_space",
            "ParkTotalNo": "total_space",
            "GetDatatime": "update_time"
        }, inplace=True)

        # filename = datetime.strftime(datetime.now(ZoneInfo('Asia/Taipei')), "%Y-%m-%d_%H-%M-%S")
        # df.to_csv(f"./{filename}mrt_parking.csv",
        #           encoding="utf-8-sig", index=False)
        # return ("OK")
        return (df)  # 可以輸出df做load之用  測試階段先直接輸出csv到local端

    @task
    def L_mrt_parking_to_sql(df):
        username_sql = os.getenv("ANDY_USERNAME_SQL")
        password_sql = os.getenv("ANDY_PASSWORD_SQL")
        server = "host.docker.internal:3306"  #docker用
        # server = "localhost:3306"
        db_name = "group2_db"
        with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:
            df.to_sql(
                name="mrt_parking",
                con=conn,
                if_exists="append",
                index=False
            )
        print("OK")
        return ("OK")


    E_df = E_mrt_parking()
    T_df = T_mrt_parking(df=E_df)
    L_mrt_parking_to_sql(df=T_df)

DAG_mrt_parking()