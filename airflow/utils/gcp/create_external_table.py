import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


def SRC_youbike_gcs_to_bq_before0504(dataset_name: str, create_table_name: str, client: bigquery.Client) -> None:
    """create external table (GCS to BQ) for you bike data(format for API before 0504) """
    query_job = client.query(
        f"""
        CREATE OR REPLACE EXTERNAL TABLE `{dataset_name}.{create_table_name}`
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
    print(f"{dataset_name}.{create_table_name} has been created.")


def SRC_youbike_gcs_to_bq_after0504(dataset_name: str, create_table_name: str, client: bigquery.Client) -> None:
    """create external table (GCS to BQ) for you bike data(format for API after 0504) """
    query_job = client.query(
        f"""
        CREATE OR REPLACE EXTERNAL TABLE `{dataset_name}.{create_table_name}`
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
    print(f"{dataset_name}.{create_table_name} has been created.")


if __name__ == "__main__":
    # BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
    BIGQUERY_CREDENTIALS_FILE_PATH = r"C:\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()
    SRC_youbike_gcs_to_bq_before0504(
        dataset_name="ANDY_ETL_SRC",
        create_table_name="SRC_youbike_before0504", client=BQ_CLIENT)
    SRC_youbike_gcs_to_bq_after0504(
        dataset_name="ANDY_ETL_SRC",
        create_table_name="SRC_youbike_after0504", client=BQ_CLIENT)
