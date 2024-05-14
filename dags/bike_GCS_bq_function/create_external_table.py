import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import db_dtypes

BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
BQ_CLIENT = bigquery.Client()


def youbike_gcs_src_to_bq_before0504(client: bigquery.Client = BQ_CLIENT) -> None:
    """create external table (GCS to BQ) for you bike data(format for API before 0504) """
    query_job = client.query(
        """
        CREATE OR REPLACE EXTERNAL TABLE `Youbike_ODS.youbike_src_before_0504`
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
            srcUpdateTime TIMESTAMP,
            updateTime TIMESTAMP,
            infoTime TIMESTAMP,
            infoDate DATE
        )
        WITH PARTITION COLUMNS
        OPTIONS (
            format = 'CSV',
            uris = ['gs://youbike_realtime/v1_data/*.csv'],
            hive_partition_uri_prefix = 'gs://youbike_realtime/v1_data/',
            skip_leading_rows = 1,
            max_bad_records = 1
        );
    """
    )

    query_job.result()
    print("youbike_gcs_src_to_bq_before0504 created.")


def youbike_gcs_src_to_bq_after0504(client: bigquery.Client = BQ_CLIENT) -> None:
    """create external table (GCS to BQ) for you bike data(format for API after 0504) """
    query_job = client.query(
        """
        CREATE OR REPLACE EXTERNAL TABLE `Youbike_ODS.youbike_src_after0504`
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
    """
    )

    query_job.result()
    print("youbike_gcs_src_to_bq_after0504 created.")


if __name__ == "__main__":
    youbike_gcs_src_to_bq_before0504()
    youbike_gcs_src_to_bq_after0504()
