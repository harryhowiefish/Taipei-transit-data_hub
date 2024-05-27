'''
this DAG includes gcs -> src -> dim for city code
'''

from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
import logging

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
PROJECT_NAME = os.environ['PROJECT_NAME']
CLIENT = bigquery.Client()

# these are some common arguments for dags
default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['mrt', 'one_time']
)
def mrt_station_src_ods_dim():

    src_name = 'SRC_mrt_station'
    ods_name = 'ODS_mrt_station'
    dim_name = 'DIM_mrt_station'
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    ods_dataset = f'{BQ_PREFIX}ETL_ODS'
    dim_dataset = f'{BQ_PREFIX}ETL_DIM'

    @python_task
    def src_to_ods():

        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE {PROJECT_NAME}.{ods_dataset}.{ods_name} as (
            SELECT
                StationID as mrt_station_id,
                StationName_Zh_tw as station_name,
                StationAddress as station_address,
                PositionLon as lng,
                PositionLat as lat,
                LocationCityCode as city_code,
                LocationTown as district,
                BikeAllowOnHoliday as bike_allow_on_holiday,
                DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
            FROM
                {PROJECT_NAME}.{src_dataset}.{src_name}

            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def ods_to_dim():
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE {PROJECT_NAME}.{dim_dataset}.{dim_name} as (
            SELECT
                *
            FROM
                {PROJECT_NAME}.{ods_dataset}.{ods_name}
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    src_to_ods() >> ods_to_dim()


mrt_station_src_ods_dim()
