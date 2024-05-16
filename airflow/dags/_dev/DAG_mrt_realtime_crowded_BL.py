import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import re
from sqlalchemy import create_engine, exc
from airflow.decorators import dag, task
# from zoneinfo import ZoneInfo
# 使用getenv拿取帳號密碼
load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["lala9456@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="DAG_mrt_realtime_crowded_BL",
    default_args=default_args,
    description="ETL MRT realtime_crowded(BL line) data to mysql",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["BL"]
)
def DAG_mrt_crowded_BL():
    @task
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
        </soap:Envelope>"""  # noqa

        response = requests.post(url=url, headers=headers, data=xmldata)
        json_df = json.loads(response.text.split("<?xml")[0])
        df = pd.DataFrame(json.loads(json_df))
        print("E_mrt_crowded_BL finished")
        return (df)

    @task
    def T_mrt_crowded_BL(df: pd.DataFrame):
        pattern = re.compile(r"[A-Za-z]+\s*")
        df["StationName"] = df["StationName"].str.replace(
            pattern, "", regex=True)
        pattern = re.compile(r"^[A-Z]+")

        def pattern_match_station_line_type(x):
            if pattern.findall(x):
                return (pattern.findall(x)[0])
            else:
                return ("")
        df["line_type"] = df["StationID"].apply(
            pattern_match_station_line_type)

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

        # filename = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
        # df.to_csv(f"./{filename}mrt_realtime_crowded_BL.csv",
        #           encoding="utf-8-sig", index=False)
        # return ("OK")
        print("T_mrt_crowded_BL finished")
        return (df)

    @task
    def L_mrt_crowded_BL(df: pd.DataFrame):
        username_sql = os.getenv("ANDY_USERNAME_SQL")
        password_sql = os.getenv("ANDY_PASSWORD_SQL")
        server = "host.docker.internal:3306"  # docker用
        # server = "localhost:3306"
        db_name = "group2_db"
        with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:  # noqa
            df.reset_index(drop=True, inplace=True)
            for i in range(len(df)):
                row = df.loc[[i], ]
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

    E_df = E_mrt_crowded_BL()
    T_df = T_mrt_crowded_BL(df=E_df)
    L_mrt_crowded_BL(df=T_df)


DAG_mrt_crowded_BL()
