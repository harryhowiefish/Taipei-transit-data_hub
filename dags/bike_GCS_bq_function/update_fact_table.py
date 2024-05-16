import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import db_dtypes


def update_data_insert_merge_into_ods(target_dataset_name: str, source_dataset_name: str, source_table_name: str, target_table_name: str, client: bigquery.Client):
    """USE MERGE FUNCTION to insert new data from src into youbike_ods"""
    query_job = client.query(
        f"""
    MERGE INTO `{target_dataset_name}.{target_table_name}` AS target
    USING `{source_dataset_name}.{source_table_name}` AS source
    ON target.bike_station_id = source.sno AND target.source_time > source.srcUpdateTime
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            bike_station_id,
            station_name, 
            district,
            make_date,
            address,
            district_eng,
            station_name_eng,
            address_eng,
            disable,
            source_time,
            infoTime,
            infoDate,
            total_space,
            aval_bike,
            lat,
            lng,
            aval_space
            )
        VALUES (
            source.sno ,
            source.sna  ,
            source.sarea  ,
            source.mday  ,
            source.ar  ,
            source.sareaen  ,
            source.snaen  ,
            source.aren  ,
            source.act  ,
            source.srcUpdateTime  ,
            source.infoTime,
            source.infoDate,
            source.total  ,
            source.available_rent_bikes ,
            source.latitude  ,
            source.longitude  ,
            source.available_return_bikes  
        );
    """
    )

    result = query_job.result()
    print(
        f"new data has been insertde into {target_dataset_name}.{target_table_name}")
    return (result)


def update_data_insert_merge_into_fact_bike_realtime(target_dataset_name: str, source_dataset_name: str, source_table_name: str, target_table_name: str, client: bigquery.Client):
    """USE MERGE FUNCTION to insert new data from youbike_ods into fact table bike_realtime"""
    query_job = client.query(
        f"""
    MERGE INTO `{target_dataset_name}.{target_table_name}` AS target
    USING `{source_dataset_name}.{source_table_name}` AS source
    ON target.bike_station_id = source.bike_station_id AND target.source_time > source.source_time
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            bike_station_id,
            aval_bike,
            aval_space,
            create_time,
            source_time
        )
        VALUES(
            source.bike_station_id,
            source.aval_bike,
            source.aval_space,
            TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 8 HOUR),
            source.source_time
        );
    """
    )
    query_job.result()
    print(
        f"new data has been insertde into {target_dataset_name}.{target_table_name}")


if __name__ == "__main__":
    # BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
    BIGQUERY_CREDENTIALS_FILE_PATH = r"C:\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()
    update_data_insert_merge_into_ods(target_dataset_name="ETL_ODS",
                                      source_dataset_name="ETL_SRC",
                                      source_table_name="SRC_youbike_after0504",
                                      target_table_name="ODS_youbike_realtime",
                                      client=BQ_CLIENT)
    update_data_insert_merge_into_fact_bike_realtime(target_dataset_name="ETL_FACT",
                                                     source_dataset_name="ETL_ODS",
                                                     source_table_name="ODS_youbike_realtime",
                                                     target_table_name="FACT_bike_realtime",
                                                     client=BQ_CLIENT)
