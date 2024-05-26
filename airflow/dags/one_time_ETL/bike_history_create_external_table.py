
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
    tags=['bike', 'one_time']
)
def bike_history_create_external_table():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    src_name = 'SRC_bike_history'

    @python_task
    def create_external_table():
        job = CLIENT.query(
            f"""
                CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{src_dataset}.{src_name}
                (
                    index INT,
                    rent_time TIMESTAMP,
                    rent_station STRING,
                    return_time TIMESTAMP,
                    return_station STRING,
                    rent STRING,
                    infodate DATE
                )
                WITH PARTITION COLUMNS
                OPTIONS (
                    format = 'CSV',
                uris = [
                        'gs://{BUCKET_TYPE}static_reference/bike_history/*.csv'
                    ],
                    hive_partition_uri_prefix = 'gs://{BUCKET_TYPE}static_reference/bike_history/',
                    skip_leading_rows = 1,
                    max_bad_records = 1
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


bike_history_create_external_table()
