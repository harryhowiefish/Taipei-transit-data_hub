from airflow.decorators import dag, python_task, branch_python
from google.cloud import bigquery
import os
import pendulum
import logging
from google.cloud.exceptions import NotFound

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
    schedule=None,
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['other', 'reoccuring']
)
def time_table_reoccuring_src_ods_dim():

    src_name = 'SRC_time_table'
    ods_name = 'ODS_time_table'
    dim_name = 'DIM_time_table'
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    ods_dataset = f'{BQ_PREFIX}ETL_ODS'
    dim_dataset = f'{BQ_PREFIX}ETL_DIM'

    @branch_python.branch_task
    def check_if_time_table_dim_exist():
        dataset = CLIENT.dataset(dim_dataset)
        table_id = dataset.table(dim_name)
        try:
            # Try to get the table
            CLIENT.get_table(table_id)
            return 'get_latest_year_in_dim'
        except NotFound:
            return 'create_ods_from_src'

    @python_task
    def create_ods_from_src():
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE `{PROJECT_NAME}.{ods_dataset}.{ods_name}` AS (
            SELECT *
            FROM
                {PROJECT_NAME}.{src_dataset}.{src_name}
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def create_dim_from_ods():
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE `{PROJECT_NAME}.{dim_dataset}.{dim_name}` AS (
            SELECT *
            FROM
                {PROJECT_NAME}.{ods_dataset}.{ods_name}
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def get_latest_year_in_dim():
        job = CLIENT.query(
            f'''
            SELECT max(year) FROM {PROJECT_NAME}.{dim_dataset}.{dim_name} LIMIT 1000
            '''  # noqa
        )
        results = job.result()
        first_row = next(results)
        first_value = first_row.values()[0]
        return first_value

    @python_task
    def src_to_ods(year):
        job = CLIENT.query(
            f'''
            INSERT INTO `{PROJECT_NAME}.{ods_dataset}.{ods_name}` (
            SELECT *

            FROM
                {PROJECT_NAME}.{src_dataset}.{src_name}
            where year > {year}
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def ods_to_dim(year):
        job = CLIENT.query(
            f'''
            INSERT INTO `{PROJECT_NAME}.{dim_dataset}.{dim_name}` (
            SELECT *

            FROM
                {PROJECT_NAME}.{ods_dataset}.{ods_name}
            where year > {year}
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return
    last_year = get_latest_year_in_dim()
    ods_from_src = create_ods_from_src()
    check_if_time_table_dim_exist() >> [ods_from_src, last_year]
    ods_from_src >> create_dim_from_ods()
    src_to_ods(last_year) >> ods_to_dim(last_year)


time_table_reoccuring_src_ods_dim()
