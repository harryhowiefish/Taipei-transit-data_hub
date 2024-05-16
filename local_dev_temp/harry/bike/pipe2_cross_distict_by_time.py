
import pandas as pd
import os
import numpy as np
from pathlib import Path

file_list = os.listdir('./bike/history')
file_list = [file for file in file_list if 'bike_usage_history' in file]

tpe_stations = pd.read_csv('./bike/TPE_bike_station.csv')
nwt_stations = pd.read_csv('./bike/NWT_bike_station.csv')
tpe_stations = tpe_stations[['station_no',
                             'name_tw', 'district_tw']].add_prefix('lend_')
nwt_stations = nwt_stations[['station_no',
                             'name_tw', 'district_tw']].add_prefix('return_')

by_station = None
by_time = None

for path in file_list:
    df = pd.read_csv(Path('bike/history', path), index_col=0)
    df.columns = ['lend_time', 'lend_station_name', 'return_time',
                  'return_station_name', 'usage_time', 'source_date']
    df['source_date'] = pd.to_datetime(df['source_date'])
    df['lend_date'] = pd.to_datetime(df['lend_time']).dt.date
    df['lend_hour'] = pd.to_datetime(df['lend_time']).dt.hour
    df['return_date'] = pd.to_datetime(df['return_time']).dt.date
    df['return_hour'] = pd.to_datetime(df['return_time']).dt.hour
    df = df.drop(['lend_time', 'return_time'], axis=1)
    df['usage_time'] = pd.to_timedelta(
        df['usage_time']).dt.total_seconds().astype('int')
    df = df.merge(tpe_stations, how='inner',
                  left_on='lend_station_name', right_on='lend_name_tw')

    df = df.merge(nwt_stations, how='inner',
                  left_on='return_station_name', right_on='return_name_tw')

    df = df[(df['lend_date'] == df['return_date']) & (df['lend_hour'] == df['return_hour'])]  # noqa

    df = df[['lend_date', 'lend_hour', 'lend_station_no', 'lend_station_name',
             'lend_district_tw', 'return_station_no', 'return_station_name',
             'return_district_tw']]

    # one_month_by_station = df.groupby(
    #     by=df.columns.to_list(), as_index=False).size()

    # one_month_by_station['day_of_week'] = pd.to_datetime(
    #     one_month_by_station['lend_date']).dt.day_name()
    # one_month_by_station['weekend'] = pd.to_datetime(
    #     one_month_by_station['lend_date']).dt.dayofweek.isin([5, 6])
    # one_month_by_station['weekend'] = np.where(
    #     one_month_by_station['weekend'], '假日', '平日')

    # one_month_by_station = one_month_by_station.rename(
    #     {'lend_date': 'date',
    #      'lend_hour': 'hour',
    #      'lend_station_no': 'lend_station_id',
    #      'lend_district_tw': 'lend_station_district',
    #      'return_station_no': 'return_station_id',
    #      'return_district_tw': 'return_station_district',
    #      'size': 'traffic_count'}, axis=1
    # )
    # one_month_by_station = one_month_by_station[['date', 'hour',
    #                                              'day_of_week',
    #                                              'weekend',
    #                                              'lend_station_id',
    #                                              'lend_station_name',
    #                                              'lend_station_district',
    #                                              'return_station_id',
    #                                              'return_station_name',
    #                                              'return_station_district',
    #                                              'traffic_count']]

    # if by_station is None:
    #     by_station = one_month_by_station.copy(deep=True)

    # else:
    #     by_station = pd.concat(
    #         [by_station, one_month_by_station], ignore_index=True)

    one_month_by_time = df.groupby(
        by=['lend_date', 'lend_hour'], as_index=False).size()

    one_month_by_time['day_of_week'] = pd.to_datetime(
        one_month_by_time['lend_date']).dt.day_name()
    one_month_by_time['weekend'] = pd.to_datetime(
        one_month_by_time['lend_date']).dt.dayofweek.isin([5, 6])
    one_month_by_time['weekend'] = np.where(
        one_month_by_time['weekend'], '假日', '平日')

    one_month_by_time = one_month_by_time.rename(
        {'lend_date': 'date',
         'lend_hour': 'hour',
         'size': 'traffic_count'}, axis=1)
    one_month_by_time = one_month_by_time[['date', 'hour',
                                           'day_of_week', 'weekend',
                                           'traffic_count'
                                           ]]

    if by_time is None:
        by_time = one_month_by_time.copy(deep=True)
    else:
        by_time = pd.concat(
            [by_time, one_month_by_time], ignore_index=True)

    print(f"finish processing: {path}")
    # print(f'final_data_size: {by_station.shape}, {by_time.shape}')

# by_station.to_csv('./bike/by_station_all_data.csv', index=False)

by_time.to_csv('./bike/by_time_all_data.csv', index=False)
