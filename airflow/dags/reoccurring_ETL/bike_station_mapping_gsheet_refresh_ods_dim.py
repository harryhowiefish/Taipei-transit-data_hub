
from airflow.decorators import dag, python_task
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pendulum
import logging
import json

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
BQ_PREFIX = ''
PROJECT_NAME = os.environ['PROJECT_NAME']

# The default cred doesn't work because scope needs to be set for creds
with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) as source:
    info = json.load(source)
credentials = service_account.Credentials.from_service_account_info(
    info, scopes=['https://www.googleapis.com/auth/cloud-platform',
                  "https://www.googleapis.com/auth/drive",
                  "https://www.googleapis.com/auth/bigquery",])

CLIENT = bigquery.Client(
    credentials=credentials)

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='@daily',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['reoccurring_ETL']
)
def bike_station_mapping_create_ods_dim():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    ods_dataset = f'{BQ_PREFIX}ETL_ODS'
    # dim_dataset = f'{BQ_PREFIX}ETL_DIM'
    src_name = 'src_bike_station_mapping_gsheet'
    ods_name = 'ODS_bike_station_mapping'
    # dim_name = 'DIM_bike_station_mapping'

    @python_task
    def src_to_ods():
        job = CLIENT.query(
            f"""CREATE OR REPLACE TABLE `{PROJECT_NAME}.{ods_dataset}.{ods_name}` as
                (
                SELECT
                    history_station_name,
                    CAST(web_station_id as INTEGER) as web_station_id,
                    web_station_name,
                    CAST(open_data_station_id  as INTEGER) as open_data_station_id,
                    open_data_station_name,
                    station_name
                FROM `{PROJECT_NAME}.{src_dataset}.{src_name}`
                );
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    # @python_task
    # def ods_to_dim():
    #     job = CLIENT.query(
    #         f"""CREATE OR REPLACE TABLE `{PROJECT_NAME}.{dim_dataset}.{dim_name}` as
    #             (
    #             SELECT *
    #             FROM `{PROJECT_NAME}.{ods_dataset}.{ods_name}`
    #             );
    #             """  # noqa
    #     )
    #     while job.done() is False:
    #         pass
    #     logging.info(job.done(), job.exception())
    #     if job.exception():
    #         raise ConnectionRefusedError
    #     return
    src_to_ods()  # >> ods_to_dim()


bike_station_mapping_create_ods_dim()
