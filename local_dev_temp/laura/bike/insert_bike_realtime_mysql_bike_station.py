'''
1. clean # %%
2. clean unnecessary code
3. group into functions (at least E/T/L)  也算是refactoring(重構)
4. write main function
--------以上都是必須要做的----------------
5. add prints or logging
6. setup parameters for modularity
7. add type hints
8. add error handling
9. refactor(重構) commonly used code
'''


from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd


def e_bike_realtime():
    path = "./bike/bile_usage_realtime.csv"
    df = pd.read_csv(path, index_col=0)
    return df


# 刪除不要的欄位
def t_bike_realtime(df):
    df = df.drop(['sbi', 'mday', 'sareaen', 'snaen',
                  'aren', 'srcUpdateTime', 'infoTime',
                  'infoDate', 'bemp', 'updateTime'
                  ], axis=1, errors='ignore')

    # 使用mapping 重新定義欄位新名稱
    mapping = {'sno': 'bike_station_id',
               'sna': 'station_name',
               'tot': 'total_space',
               'sarea': 'district',
               'lat': 'lat',
               'lng': 'lng',
               'ar': 'address',
               'act': 'disable'
               }

    # 將重新定義的新欄位名稱 套用到df
    # ,axis=1 ->指定‘欄’
    df = df.rename(mapping, axis=1)

    # 新增欄位'city_code'
    # 將欄位'city_code' 給統一值'TPE'
    df['city_code'] = 'TPE'
    return df


def t_bike_realtime_station_name(df):
    def rename_station(text):
        prefix = 'YouBike2.0_'
        result = text.replace(prefix, '')
        return result

    df['station_name'] = df['station_name'].apply(rename_station)
    return df


# dataframe 整理好-> 連結sql


def l_bike_realtime(df):
    username = quote_plus("root")
    password = quote_plus("password")
    server = "localhost:55000"
    db_name = "group2_db"

    conn = create_engine(
        f"mysql+pymysql://{username}:{password}@{server}/{db_name}",).connect()  # noqa

    # 把dataframe insert to sql
    df.to_sql('bike_station', conn,
              if_exists='append', index=False)
    return


df = e_bike_realtime()
df_transformed = t_bike_realtime(df)
df_transformed_rename = t_bike_realtime_station_name(df_transformed)
l_bike_realtime(df_transformed_rename)
# 使用自定义函数处理 DataFrame
