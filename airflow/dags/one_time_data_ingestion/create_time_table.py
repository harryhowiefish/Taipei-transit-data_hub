import pandas as pd
import datetime
import numpy as np
from google.cloud import storage
from airflow.decorators import dag, python_task
from utils.gcp import gcs
import os
from pendulum.datetime import DateTime
import pendulum


BUCKET_TYPE = os.environ['BUCKET_TYPE']


def create_single_year_table(year: int) -> pd.DataFrame:
    date_table = pd.DataFrame(
        pd.date_range(
            datetime.date(year, 1, 1),
            datetime.date(year + 1, 12, 31)),
        columns=['date'])
    date_table['day_of_week'] = date_table['date'].dt.day_of_week
    date_table['day_of_year'] = date_table['date'].dt.day_of_year

    date_table['weekend'] = date_table['date'].dt.day_of_week.isin([5, 6])
    date_table['weekend'] = np.where(date_table['weekend'], '假日', '平日')
    date_table['year'] = date_table['date'].dt.year
    date_table['quarter'] = date_table['date'].dt.quarter
    date_table['month'] = date_table['date'].dt.month
    date_table['day'] = date_table['date'].dt.day
    date_table['days_in_month'] = date_table['date'].dt.days_in_month
    date_table['day_name_en'] = date_table['date'].dt.day_name()
    # date_table['day_name_zhtw'] = date_table['date'].dt.day_name('zh_TW.utf8')
    date_table['month_name_en'] = date_table['date'].dt.month_name()
    return date_table


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='0 0 25 12 *',
    start_date=pendulum.datetime(2018, 1, 1, tz='Asia/Taipei'),
    tags=['one_time_data_ingestion'],
    catchup=True
)
def create_time_table():

    @python_task
    def print_execution_date(execution_date=None):
        print(f"The logical date of this DAG run is: {execution_date}")

    @python_task
    def create_dataframes(execution_date: DateTime = None) -> dict:
        next_year = execution_date.add(years=1).year
        df = create_single_year_table(next_year)
        data = {'year': next_year, 'data': df}
        return data

    @python_task
    def save_to_gcs(data: dict[str, pd.DataFrame]) -> None:
        client = storage.Client()
        gcs.upload_df_to_gcs(client=client,
                             bucket_name=f'{BUCKET_TYPE}static_reference',
                             blob_name=f"time_tables/{data['year']}.csv",
                             df=data['data'])
        return

    df = create_dataframes()
    save_to_gcs(df)
    print_execution_date() >> df


create_time_table()
