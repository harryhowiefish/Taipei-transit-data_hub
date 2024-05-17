import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import geopy.distance
import numpy as np
from pathlib import Path
from tqdm import tqdm


def query_bq_to_df(client: bigquery.Client, sql_query: str) -> pd.DataFrame:
    try:
        query_job = client.query(sql_query)
        return query_job.to_dataframe()  # Convert result to DataFrame
    except Exception as e:
        raise Exception(f"Failed to query bigquery table, reason: {e}")


def get_mrt_df(client):
    mrt_sql_query = """  
        SELECT mrt_station_id,lat,lng
        FROM `MRT_GCS_to_BQ_SRC_ODS_DIM.DIM_MRT_static_data`
    """
    return (query_bq_to_df(client=client, sql_query=mrt_sql_query))


def get_youbike_df(client):
    youbike_df_sql_query = """  
        SELECT  bike_station_id,lat,lng 
        FROM `ETL_DIM.DIM_bike_station`
    """
    return (query_bq_to_df(client=client, sql_query=youbike_df_sql_query))


def get_bus_df(client):
    bus_sql_query = """  
        SELECT  bus_station_id,lat,lng
        FROM `BUS_GCS_to_BQ_SRC_ODS_DIM.DIM_Bus_static_data`
    """
    return (query_bq_to_df(client=client, sql_query=bus_sql_query))


def calculate_dis(df, location2):
    location1 = (df["lat"], df["lng"])
    return (geopy.distance.geodesic(location1, location2).kilometers)


def create_bike_mrt_distance(youbike_df: pd.DataFrame, mrt_df: pd.DataFrame):
    for i in tqdm(range(len(mrt_df))):
        mrt_station_id = mrt_df.loc[i, "mrt_station_id"]
        location_mrt = (mrt_df.loc[i, "lat"], mrt_df.loc[i, "lng"])
        youbike_df[mrt_station_id] = youbike_df.apply(
            lambda df: calculate_dis(df, location_mrt), axis=1)
        youbike_mrt_distance = youbike_df.melt(id_vars=[
            "bike_station_id",	"lat", "lng"], var_name="mrt_station_id", value_name="distance")
        youbike_mrt_distance = youbike_mrt_distance.loc[:, [
            "bike_station_id", "mrt_station_id", "distance"]]
    return (youbike_mrt_distance)


def create_bike_bus_distance(youbike_df: pd.DataFrame, bus_df: pd.DataFrame):
    for i in tqdm(range(len(bus_df))):
        bus_station_id = bus_df.loc[i, "bus_station_id"]
        location_bus = (bus_df.loc[i, "lat"], bus_df.loc[i, "lng"])
        youbike_df[bus_station_id] = youbike_df.apply(
            lambda df: calculate_dis(df, location_bus), axis=1)
        youbike_bus_distance = youbike_df.melt(id_vars=[
            "bike_station_id",	"lat", "lng"], var_name="bus_station_id", value_name="distance")
        youbike_bus_distance = youbike_bus_distance.loc[:, [
            "bike_station_id", "bus_station_id", "distance"]]
    return (youbike_bus_distance)

########################################################


def upload_df_to_bq(
    client: bigquery.Client,
    df: pd.DataFrame,
    dataset_name: str,
    table_name: str,
    schema,
    filetype: str = "parquet",
) -> bool:
    """
    Upload a pandas dataframe to bigquery.

    Args:
        client (bigquery.Client): The client to use to upload to bigquery.
        df (pd.DataFrame): The dataframe to upload.
        dataset_name (str): The name of the dataset to upload to.
        table_name (str): The name of the table to upload to.
        schema (List[bigquery.SchemaField], optional): The schema of the table to upload to. Default is None.
                                                        If None, use the default schema (automatic-detect).
        filetype (str): The type of the file to download. Default is "parquet". Can be "parquet" or "csv" or "jsonl".

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    dataset_id = client.dataset(dataset_name)
    table_id = dataset_id.table(table_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if filetype == "parquet":
        job_config.source_format = bigquery.SourceFormat.PARQUET
    elif filetype == "csv":
        job_config.source_format = bigquery.SourceFormat.CSV
    elif filetype == "jsonl":
        job_config.source_format = bigquery.SourceFormat.JSONL
    else:
        raise ValueError(
            f"Invalid filetype: {filetype}. Please specify 'parquet' or 'csv' or 'jsonl'."
        )
    if schema:
        job_config.schema = schema

    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        table = client.get_table(table_id)
        print(f"Table {table.table_id} created with {table.num_rows} rows.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload df to bigquery, reason: {e}")
###############################################################################


if __name__ == "__main__":
    # BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\dev_TIR_group2\Taipei-transit-data_hub\airflow\dags\harry_GCS_BigQuery_write_cred.json"
    BIGQUERY_CREDENTIALS_FILE_PATH = r"C:\dev_TIR101\Taipei-transit-data_hub\airflow\dags\harry_GCS_BigQuery_write_cred.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()
    # mrt
    mrt_df = get_mrt_df(client=BQ_CLIENT)
    youbike_df = get_youbike_df(client=BQ_CLIENT)
    youbike_mrt_distance = create_bike_mrt_distance(
        mrt_df=mrt_df, youbike_df=youbike_df)
    youbike_mrt_distance.to_csv(
        "youbike_mrt_distance.csv", index=False, encoding="utf-8-sig")

    youbike_mrt_distance_schema = [
        bigquery.SchemaField("bike_station_id", "INT64"),
        bigquery.SchemaField("mrt_station_id", "STRING"),
        bigquery.SchemaField("distance", "FLOAT")
    ]
    upload_df_to_bq(client=BQ_CLIENT,
                    df=youbike_mrt_distance,
                    dataset_name="ETL_DIM",
                    table_name="DIM_youbike_mrt_distance",
                    schema=youbike_mrt_distance_schema,
                    filetype="csv",)
    # bus
    bus_df = get_bus_df(client=BQ_CLIENT)
    youbike_bus_distance = create_bike_bus_distance(
        youbike_df=youbike_df, bus_df=bus_df)
    youbike_bus_distance.to_csv(
        "youbike_bus_distance.csv", index=False, encoding="utf-8-sig")
    youbike_bus_distance_schema = [
        bigquery.SchemaField("bike_station_id", "INT64"),
        bigquery.SchemaField("bus_station_id", "STRING"),
        bigquery.SchemaField("distance", "FLOAT")
    ]
    upload_df_to_bq(client=BQ_CLIENT,
                    df=youbike_bus_distance,
                    dataset_name="ETL_DIM",
                    table_name="DIM_youbike_bus_distance",
                    schema=youbike_bus_distance_schema,
                    filetype="csv",)
