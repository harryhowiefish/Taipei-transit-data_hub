
from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
import logging

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
# BUCKET_TYPE = os.environ['BUCKET_TYPE']
BUCKET_TYPE = ''

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
def bike_realtime_create_external_tables():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'

    @python_task
    def create_external_table_before_0503():
        src_name = 'SRC_bike_realtime_before_0503'
        job = CLIENT.query(
            f"""
            CREATE OR REPLACE EXTERNAL TABLE `{src_dataset}.{src_name}`
            (
                sno INT,
                sna STRING,
                tot INT,
                sbi INT,
                sarea STRING,
                mday TIMESTAMP,
                lat FLOAT64,
                lng FLOAT64,
                ar STRING,
                sareaen STRING,
                snaen STRING,
                aren STRING,
                bemp INT,
                act INT,
                srcUpdateTime TIMESTAMP(),
                updateTime TIMESTAMP,
                infoTime TIMESTAMP,
                infoDate DATE
            )
            WITH PARTITION COLUMNS
            OPTIONS (
                format = 'CSV',
                uris = ['gs://{BUCKET_TYPE}youbike_realtime/v1_data/*.csv'],
                hive_partition_uri_prefix = 'gs://{BUCKET_TYPE}youbike_realtime/v1_data/',
                skip_leading_rows = 1,
                max_bad_records = 1
            );
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def create_external_table_0503():
        src_name = 'SRC_bike_realtime_0503'
        job = CLIENT.query(
            f"""
            CREATE OR REPLACE EXTERNAL TABLE `{src_dataset}.{src_name}`
            (
                sno INT,
                sna STRING,
                tot FLOAT64,
                sbi FLOAT64,
                sarea STRING,
                mday TIMESTAMP,
                lat FLOAT64,
                lng FLOAT64,
                ar STRING,
                sareaen STRING,
                snaen STRING,
                aren STRING,
                bemp FLOAT64,
                act INT,
                srcUpdateTime TIMESTAMP,
                updateTime TIMESTAMP,
                infoTime TIMESTAMP,
                infoDate DATE,
                total FLOAT64,
                available_rent_bikes FLOAT64,
                latitude FLOAT64,
                longitude FLOAT64,
                available_return_bikes FLOAT64
            )
            WITH PARTITION COLUMNS
            OPTIONS (
                format = 'CSV',
                uris = [
                    'gs://{BUCKET_TYPE}youbike_realtime/v1_v2_transition_data/*.csv'],
                hive_partition_uri_prefix = 'gs://{BUCKET_TYPE}youbike_realtime/v1_v2_transition_data/',
                skip_leading_rows = 1,
                max_bad_records = 1
            );
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def create_external_table_after_0503():
        src_name = 'SRC_bike_realtime_after_0503'
        job = CLIENT.query(
            f"""
            CREATE OR REPLACE EXTERNAL TABLE `{src_dataset}.{src_name}`
            (
                sno INT,
                sna  STRING,
                sarea STRING,
                mday TIMESTAMP,
                ar STRING,
                sareaen STRING,
                snaen  STRING,
                aren STRING,
                act INT,
                srcUpdateTime TIMESTAMP,
                updateTime TIMESTAMP,
                infoTime TIMESTAMP,
                infoDate DATE,
                total INT,
                available_rent_bikes INT,
                latitude FLOAT64,
                longitude FLOAT64,
                available_return_bikes INT
            )
            WITH PARTITION COLUMNS
            OPTIONS (
                format = 'CSV',
                uris = ['gs://youbike_realtime/v2_data/*.csv'],
                hive_partition_uri_prefix = 'gs://youbike_realtime/v2_data/',
                skip_leading_rows = 1,
                max_bad_records = 1
            );
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    create_external_table_before_0503()
    create_external_table_0503()
    create_external_table_after_0503()


bike_realtime_create_external_tables()
