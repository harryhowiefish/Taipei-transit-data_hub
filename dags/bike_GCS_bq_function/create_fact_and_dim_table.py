import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import db_dtypes

BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
BQ_CLIENT = bigquery.Client()


def youbike_dim_bike_station(client: bigquery.Client = BQ_CLIENT):
    """create dimention table for bike station information"""
    query_job = client.query(
        """
    CREATE OR REPLACE TABLE `Youbike_ODS.bike_station` AS
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
        FROM `Youbike_ODS.youbike_ods`) AS t1
    WHERE t1.row_num=1
    ;
    """
    )
    query_job.result()
    print(f"Youbike_ODS.youbike_dim_bike_station has been created")


def youbike_fact_bike_realtime(client: bigquery.Client = BQ_CLIENT):
    """create or update ods_bike_realtime"""
    query_job = client.query(
        """
    CREATE OR REPLACE TABLE `Youbike_ODS.bike_realtime` AS
    SELECT 
        `bike_station_id`,
        `aval_bike`,
        `aval_space`,
         TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 8 HOUR) AS `create_time`,
        `source_time` 
    FROM `Youbike_ODS.youbike_ods`;
    """
    )
    query_job.result()
    print(f"Youbike_ODS.bike_realtime has been created")


if __name__ == "__main__":
    youbike_dim_bike_station()
    youbike_fact_bike_realtime()
