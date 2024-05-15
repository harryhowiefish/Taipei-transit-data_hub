import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import db_dtypes


def DIM_youbike_bike_station_create(dataset_name: str, create_table_name: str, ods_table_name: str, client: bigquery.Client):
    """create dimention table for bike station information"""
    query_job = client.query(
        f"""
    CREATE OR REPLACE TABLE `{dataset_name}.{create_table_name}` AS
    SELECT 
        `bike_station_id`,
        `station_name`,
        `total_space`,
        `lat`,
        `lng`,
        `district` ,
        `address`,
        `disable`,
         TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 8 HOUR) AS `create_time`,
        `source_time` 
    FROM
        (SELECT 
            * ,
            ROW_NUMBER() OVER (PARTITION by `bike_station_id` ORDER BY `source_time` DESC) AS `row_num`
        FROM `{dataset_name}.{ods_table_name}`) AS t1
    WHERE t1.row_num=1
    ;
    """
    )
    query_job.result()
    print(f"{dataset_name}.{create_table_name} has been created")


def FACT_youbike_bike_realtime_create(dataset_name: str, create_table_name: str, ods_table_name: str, client: bigquery.Client):
    """create or update ods_bike_realtime"""
    query_job = client.query(
        f"""
    CREATE OR REPLACE TABLE `{dataset_name}.{create_table_name}` AS
    SELECT 
        `bike_station_id`,
        `aval_bike`,
        `aval_space`,
         TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 8 HOUR) AS `create_time`,
        `source_time` 
    FROM `{dataset_name}.{ods_table_name}`;
    """
    )
    query_job.result()
    print(f"{dataset_name}.{create_table_name} has been created")


if __name__ == "__main__":
    BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()
    DIM_youbike_bike_station_create(dataset_name="Youbike",
                                    create_table_name="DIM_bike_station",
                                    ods_table_name="ODS_youbike_realtime",
                                    client=BQ_CLIENT)
    FACT_youbike_bike_realtime_create(dataset_name="Youbike",
                                      create_table_name="FACT_bike_realtime",
                                      ods_table_name="ODS_youbike_realtime",
                                      client=BQ_CLIENT)
