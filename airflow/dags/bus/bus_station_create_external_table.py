
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
    tags=['bus', 'one_time']
)
def bus_station_create_external_table():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    src_name = 'SRC_bus_station'

    @python_task
    def create_external_table():
        job = CLIENT.query(
            f"""
                CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{src_dataset}.{src_name} 
                (
                StationUID STRING,
                StationID INT, 
                StationAddress STRING, 
                Stops STRING,
                LocationCityCode STRING,
                Bearing STRING, 
                UpdateTime TIMESTAMP,
                VersionID STRING,
                `StationName` STRING,
                `PositionLon` FLOAT64,
                `PositionLat` FLOAT64, 
                `GeoHash` STRING
                )
                OPTIONS (
                    format = 'CSV',
                uris = [
                        'gs://{BUCKET_TYPE}static_reference/bus/tpe_station/*.csv'
                    ],
                    skip_leading_rows = 1,
                    max_bad_records = 0,
                    allow_quoted_newlines=true
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


bus_station_create_external_table()
