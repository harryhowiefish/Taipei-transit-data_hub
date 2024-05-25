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
def city_code_src_dim():

    src_name = 'SRC_city_code'
    ods_name = 'ODS_city_code'
    dim_name = 'DIM_city_code'

    @python_task
    def src_to_ods():
        source_dataset = f'{BQ_PREFIX}ETL_SRC'
        target_dataset = f'{BQ_PREFIX}ETL_ODS'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE {PROJECT_NAME}.{target_dataset}.{ods_name} as (
            SELECT
                city_code,city_name
            FROM
                {PROJECT_NAME}.{source_dataset}.{src_name}

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
        source_dataset = f'{BQ_PREFIX}ETL_ODS'
        target_dataset = f'{BQ_PREFIX}ETL_DIM'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE {PROJECT_NAME}.{target_dataset}.{dim_name} as (
            SELECT
                city_code,city_name
            FROM
                {PROJECT_NAME}.{source_dataset}.{ods_name}

            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    src_to_ods() >> ods_to_dim()


city_code_src_dim()
