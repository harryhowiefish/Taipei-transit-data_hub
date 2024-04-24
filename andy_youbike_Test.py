import requests
import json
import pandas as pd
from datetime import datetime



class YoubikeAPI():
    """"
        no(站點代號)、
        sna(場站中文名稱)、
        tot(場站總停車格)、
        sbi(場站目前車輛數量)、
        sarea(場站區域)、
        mday(資料更新時間)、
        lat(緯度)、
        lng(經度)、
        ar(地點)、
        sareaen(場站區域英文)、
        snaen(場站名稱英文)、
        aren(地址英文)、
        bemp(空位數量)、
        act(全站禁用狀態)、
        srcUpdateTime(YouBike2.0系統發布資料更新的時間)、
        updateTime(大數據平台經過處理後將資料存入DB的時間)、
        infoTime(各場站來源資料更新時間)、
        infoDate(各場站來源資料更新時間)
        """
    def __init__(self ,city):
        self.city = city
    def GetData(self):  
        if self.city =="台北":
            url = "https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json"
            RequestsData = requests.get(url)
            data = json.loads(RequestsData.text)
            res = pd.DataFrame(data)
            now = datetime.now()
            FileSaveDate = now.strftime("%Y%m%d")
            res.to_csv(f"{FileSaveDate}{self.city}youbike.csv" ,index = False , encoding = "utf-8-sig")
            print("已經輸出資料在當前路徑")
            return(res)
        elif self.city=="高雄":
            url = "https://api.kcg.gov.tw/api/service/Get/b4dd9c40-9027-4125-8666-06bef1756092"
            RequestsData = requests.get(url)
            data = json.loads(RequestsData.text)
            print(data)
            res = pd.DataFrame(data["data"]["retVal"])
            now = datetime.now()
            FileSaveDate = now.strftime("%Y%m%d")
            res.to_csv(f"{FileSaveDate}{self.city}youbike.csv" ,index=False , encoding="utf-8-sig")
            print("已經輸出資料在當前路徑")
            return(res)
        else:
            print("還沒支援別的縣市")
            return
        
def main():
    #YoubikeAPI.GetData(city="台北")
    youbike_instance = YoubikeAPI(city="高雄")
    youbike_instance.GetData()
    
main()
