
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
def bus_station_geocode_create_external_table():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    src_name = 'SRC_bus_geocoding'

    @python_task
    def create_external_table():
        job = CLIENT.query(
            f"""
                CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{src_dataset}.{src_name} 
                (
                bus_station_id INT,
                address STRING, 
                district STRING, 
                subarea STRING,
                )
                OPTIONS (
                    format = 'JSON',
                uris = [
                        'gs://{BUCKET_TYPE}static_reference/bus/bus_geocoding_json/*.json'
                    ]
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


bus_station_geocode_create_external_table()
