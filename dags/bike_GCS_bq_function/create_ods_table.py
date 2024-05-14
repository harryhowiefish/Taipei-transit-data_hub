import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import db_dtypes

BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
BQ_CLIENT = bigquery.Client()


def youbike_ods_before0504_create(client: bigquery.Client = BQ_CLIENT):
    query_job = client.query(
        """
    CREATE OR REPLACE TABLE `Youbike_ODS.youbike_ods_before0504` AS
    SELECT
        sno AS bike_station_id,
        sna AS station_name,
        sarea AS district,
        mday AS make_date,
        ar AS address,
        sareaen AS district_eng,
        snaen AS station_name_eng,
        aren AS address_eng,
        act AS disable,
        srcUpdateTime AS source_time,
        #updateTime AS create_time,
        infoTime,
        infoDate,
        tot AS total_space,
        sbi AS aval_bike,
        lat,
        lng,
        bemp AS aval_space
    FROM `Youbike_ODS.youbike_src_before_0504`;
    """
    )
    query_job.result()
    print(f"Youbike_ODS.youbike_ods_before0504 has been created")


def youbike_ods_after0504_create(client: bigquery.Client = BQ_CLIENT):
    query_job = client.query(
        """
    CREATE OR REPLACE TABLE `Youbike_ODS.youbike_ods_after0504` AS
    SELECT
        sno AS bike_station_id,
        sna AS station_name,
        sarea AS district,
        mday AS make_date,
        ar AS address,
        sareaen AS district_eng,
        snaen AS station_name_eng,
        aren AS address_eng,
        act AS disable,
        srcUpdateTime AS source_time,
        #updateTime AS create_time,
        infoTime,
        infoDate,
        total AS total_space,
        available_rent_bikes AS aval_bike,
        latitude AS lat,
        longitude AS lng,
        available_return_bikes AS aval_space
    FROM `Youbike_ODS.youbike_src_after0504`;
    """
    )
    query_job.result()
    print(f"Youbike_ODS.youbike_ods_after0504 has been created")


def youbike_ods_create(client: bigquery.Client = BQ_CLIENT):
    """create youbike ods table(combine two table(before0504 and after 0504))"""
    query_job = client.query(
        """
    CREATE OR REPLACE TABLE `Youbike_ODS.youbike_ods` AS
        (   SELECT * 
                FROM `Youbike_ODS.youbike_ods_before0504`
            UNION ALL
            SELECT * 
                FROM `Youbike_ODS.youbike_ods_after0504`
        )
        ;
    """
    )
    query_job.result()
    print(f"Youbike_ODS.youbike_ods has been created")


if __name__ == "__main__":
    youbike_ods_before0504_create()
    youbike_ods_after0504_create()
    youbike_ods_create()
