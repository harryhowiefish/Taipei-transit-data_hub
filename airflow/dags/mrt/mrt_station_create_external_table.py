
from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
import logging

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
BUCKET_TYPE = os.environ['BUCKET_TYPE']
# BUCKET_TYPE = ''

PROJECT_NAME = os.environ['PROJECT_NAME']
CLIENT = bigquery.Client()

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
def mrt_station_create_external_table():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    src_name = 'SRC_mrt_station'

    @python_task
    def create_external_table():
        job = CLIENT.query(
            f"""
                CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{src_dataset}.{src_name} 
                (
                StationUID string,
                StationID string,
                StationAddress string,
                BikeAllowOnHoliday boolean,
                SrcUpdateTime timestamp,
                UpdateTime timestamp,
                VersionID int,
                LocationCity string,
                LocationCityCode string,
                LocationTown string,
                LocationTownCode string,
                StationName_Zh_tw string,
                StationName_En string,
                PositionLon float64,
                PositionLat float64,
                GeoHash string
                )
                OPTIONS (
                    format = 'CSV',
                uris = [
                        'gs://{BUCKET_TYPE}static_reference/mrt/trtc_station/*.csv'
                    ],
                    skip_leading_rows = 1,
                    max_bad_records = 0
                )
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    create_external_table()


mrt_station_create_external_table()
