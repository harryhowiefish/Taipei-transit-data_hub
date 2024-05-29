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
    tags=['other', 'one_time']
)
def city_code_create_external_table():

    src_name = 'SRC_city_code'

    @python_task
    def create_external_table():
        target_dataset = f'{BQ_PREFIX}ETL_SRC'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{target_dataset}.{src_name} (
            city_code STRING NOT NULL,
            city_name STRING NOT NULL,
            ) OPTIONS (
                format = 'CSV',
                uris = ['gs://static_reference/additional_references/city_code.csv'],
                skip_leading_rows = 1);
                '''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    create_external_table()


city_code_create_external_table()
