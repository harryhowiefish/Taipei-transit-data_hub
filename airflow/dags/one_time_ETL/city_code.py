'''
this DAG includes gcs -> src -> dim for city code
'''

from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
import logging

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
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
    start_date=pendulum.today(),
    tags=['one_time_ETL']
)
def etl_city_code():

    src_name = 'src_city_code'
    dim_name = 'dim_city_code'

    @python_task
    def gcs_to_src():
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{BQ_PREFIX}ETL_SRC.{src_name} (
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

    @python_task
    def src_to_dim():

        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE {PROJECT_NAME}.{BQ_PREFIX}ETL_SRC.{dim_name} as (
            SELECT
                city_code,city_name
            FROM
                {PROJECT_NAME}.{BQ_PREFIX}ETL_SRC.{src_name}

            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    gcs_to_src() >> src_to_dim()


etl_city_code()
