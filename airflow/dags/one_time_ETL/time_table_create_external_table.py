from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
import logging

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
PROJECT_NAME = os.environ['PROJECT_NAME']
BUCKET_TYPE = os.environ['BUCKET_TYPE']
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
    tags=['other', 'one_time']
)
def time_table_create_external_table():

    src_name = 'SRC_time_table'

    @python_task
    def gcs_to_src():
        target_dataset = f'{BQ_PREFIX}ETL_SRC'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{target_dataset}.{src_name} (
            date DATE NOT NULL,
            day_of_week INTEGER NOT NULL,
            day_of_year INTEGER NOT NULL,
            weekend STRING NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            days_in_month INTEGER NOT NULL,
            day_name_en STRING NOT NULL,
            month_name_en STRING NOT NULL,

            ) OPTIONS (
                format = 'CSV',
                uris = ['gs://{BUCKET_TYPE}static_reference/time_tables/*.csv'],
                skip_leading_rows = 1);
                '''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    gcs_to_src()


time_table_create_external_table()
