
from airflow.decorators import dag, python_task
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import pendulum
import logging
import json

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
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
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['reoccurring_ETL']
)
def bike_mapping_gsheet_create_external_table():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'

    src_name = 'src_bike_station_mapping_gsheet'

    @python_task
    def create_external_table():
        job = CLIENT.query(
            f"""
                CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_NAME}.{src_dataset}.{src_name}` (
                history STRING,
                web_station_id INTEGER,
                web_station_name STRING,
                open_data_station_id INTEGER,
                open_data_station_name STRING,
                station_name STRING)
                OPTIONS (
                format = 'GOOGLE_SHEETS',
                skip_leading_rows = 1,  -- Skip header row, adjust if needed
                sheet_range="bike_mapping",
                uris = ['https://docs.google.com/spreadsheets/d/1U6C0Zr2O7zHCrqy3pU0nU8R1qAko9X7HC04uWodTiQ8/edit#gid=1772264831']
                );
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    create_external_table()


bike_mapping_gsheet_create_external_table()
