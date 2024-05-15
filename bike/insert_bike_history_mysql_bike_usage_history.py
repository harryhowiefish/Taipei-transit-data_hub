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

import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd


def e_bike_history(path):
    # path = "./bike/2023_7_bike_usage_history.csv"
    df = pd.read_csv(path, index_col=0)
    return df


def t_bike_history(df):
    # 重新給欄位名稱 （另一個作法是用mapping）
    df.columns = ['lend_time', 'lend_station_name', 'return_time',
                  'return_station_name', 'usage_time', 'source_date']
    # source_date type 改 datetime
    df['source_date'] = pd.to_datetime(df['source_date'])
    # ’lend_time‘ type 改 datetime 只取data 並另外存在'lend_date'
    df['lend_date'] = pd.to_datetime(df['lend_time']).dt.date
    # ’lend_time‘ type 改 datetime 只取hour 並另外存在'lend_hour'
    df['lend_hour'] = pd.to_datetime(df['lend_time']).dt.hour
    # ’return_time‘ type 改 datetime 只取data 並另外存在'return_date'
    df['return_date'] = pd.to_datetime(df['return_time']).dt.date
    # ’return_time‘ type 改 datetime 只取hour 並另外存在'return_hour'
    df['return_hour'] = pd.to_datetime(df['return_time']).dt.hour
    # 刪除不要的欄位
    # ,axis=1 ->宣告刪除的是欄
    df = df.drop(['lend_time', 'return_time'], axis=1)
    df['usage_time'] = pd.to_timedelta(df['usage_time'])
    df['usage_time'] = df['usage_time'].dt.total_seconds().astype('int')
    return df


def get_data_from_db():
    username = quote_plus("root")
    password = quote_plus("password")
    server = "localhost:55000"
    db_name = "group2_db"

    sql = """
        select bike_station_id , station_name
        from bike_station;
    """
    with create_engine(f"mysql+pymysql://{username}:{password}@{server}/{db_name}",).connect() as conn:  # noqa
        df_sql = pd.read_sql(sql, conn)
    return df_sql


def t_bilke_history_name_mapping(df, mapping):
    def get_station_id(station_name):
        return mapping.get(station_name, -1)
    df['lend_station_name'] = df['lend_station_name'].apply(get_station_id)
    df = df.rename({'lend_station_name': 'lend_station_id'}, axis=1)
    df['return_station_name'] = df['return_station_name'].apply(get_station_id)
    df = df.rename({'return_station_name': 'return_station_id'}, axis=1)
    return df


def l_bike_history(df):
    username = quote_plus("root")
    password = quote_plus("password")
    server = "localhost:55000"
    db_name = "group2_db"
    with create_engine(
            f"mysql+pymysql://{username}:{password}@{server}/{db_name}",).connect() as conn:  # noqa
        df.to_sql('bike_usage_history', conn,
                  if_exists='append', index=False)
    return


def main():
    file_list = os.listdir('bike')
    filetered = [file for file in file_list if 'csv' in file]
    for path in filetered:
        print(f'reading_file: {path}')
        # 看資料夾有哪些csv
        # 跑for loop 把每個個csv跑下面的流程
        df = e_bike_history()
        df_transformed = t_bike_history(df)
        df_sql = get_data_from_db()
        mapping = pd.Series(df_sql['bike_station_id'].values,
                            index=df_sql['station_name']).to_dict()
        df_transformed_mapped = t_bilke_history_name_mapping(
            df_transformed, mapping)
        l_bike_history(df_transformed_mapped)
        print("success")


if __name__ == '__main__':
    main()
