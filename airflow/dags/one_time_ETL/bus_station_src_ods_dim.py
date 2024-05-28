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
    tags=['one_time_ETL']
)
def bus_station_src_ods_dim():

    main_src_name = 'SRC_bus_station'
    main_ods_name = 'ODS_bus_station'
    geocoding_src_name = 'SRC_bus_geocoding'
    geocoding_ods_name = 'SRC_bus_geocoding'
    dim_name = 'DIM_bus_station'
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    ods_dataset = f'{BQ_PREFIX}ETL_ODS'
    dim_dataset = f'{BQ_PREFIX}ETL_DIM'

    @python_task
    def main_src_to_ods():

        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE {PROJECT_NAME}.{ods_dataset}.{main_ods_name} as (
            SELECT
                StationID as bus_station_id,
                StationName as station_name,
                StationAddress as address,
                LocationCityCode as city_code,
                PositionLat as lat,
                PositionLon as lng,
                Bearing as bearing,
                DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
            FROM
                {PROJECT_NAME}.{src_dataset}.{main_src_name}

            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def geocoding_src_to_ods():
        source_dataset = f'{BQ_PREFIX}ETL_SRC'
        target_dataset = f'{BQ_PREFIX}ETL_ODS'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE {PROJECT_NAME}.{target_dataset}.{geocoding_ods_name} as (
            SELECT
                *,
                DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
            FROM
                {PROJECT_NAME}.{source_dataset}.{geocoding_src_name}

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
                t1.*,
                t2.district as district
            FROM
                {PROJECT_NAME}.{ods_dataset}.{main_ods_name} as t1
            LEFT JOIN {PROJECT_NAME}.{ods_dataset}.{geocoding_ods_name} as t2
            USING(bus_station_id)
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    [main_src_to_ods(), geocoding_src_to_ods()] >> ods_to_dim()


bus_station_src_ods_dim()
