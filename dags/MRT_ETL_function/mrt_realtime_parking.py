import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os
from datetime import datetime
import re
from sqlalchemy import create_engine
from zoneinfo import ZoneInfo
# 使用getenv拿取帳號密碼
load_dotenv()


def E_mrt_realtime_parking():
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

    now = datetime.strftime(datetime.now(
        ZoneInfo('Asia/Taipei')), "%Y-%m-%d %H:%M:%S")
    response = requests.post(url=url, headers=headers,
                             data=xmldata.encode("utf-8"))
    df = pd.DataFrame(json.loads(response.text.split("<?xml")[0]))
    df["GetDatatime"] = now
    print("E_mrt_realtime_parking finished")
    return (df)


def T_mrt_realtime_parking(df: pd.DataFrame):
    df = df.loc[:, ["ParkName", "ParkNowNo", "GetDatatime"]]
    df.rename(columns={
        "ParkName": "park_name",
        "ParkNowNo": "available_space",
        "GetDatatime": "update_time"
    }, inplace=True)

    filename = datetime.strftime(datetime.now(), "%Y-%m-%d_%H-%M-%S")
    # df.to_csv(f"./{filename}mrt_parking.csv",
    #           encoding="utf-8-sig", index=False)
    # return ("OK")
    print("T_mrt_realtime_parking finished")
    return (df)  # 可以輸出df做load之用  測試階段先直接輸出csv到local端


def L_mrt_realtime_parking(df: pd.DataFrame, port: str = "docker"):
    username_sql = os.getenv("ANDY_USERNAME_SQL")
    password_sql = os.getenv("ANDY_PASSWORD_SQL")
    if port == "docker":
        server = "host.docker.internal:3306"  # docker用
    else:
        server = f"localhost:{port}"
    db_name = "group2_db"
    with create_engine(f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:
        df.to_sql(
            name="mrt_realtime_parking",
            con=conn,
            if_exists="append",
            index=False
        )
    print("L_mrt_realtime_parking finished")
    return ("L_mrt_realtime_parking finished")


if __name__ == "__main__":
    E_df = E_mrt_realtime_parking()
    T_df = T_mrt_realtime_parking(df=E_df)
    L_mrt_realtime_parking(df=T_df, port="3306")
