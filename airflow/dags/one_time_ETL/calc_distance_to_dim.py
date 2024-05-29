'''table name change still required'''
from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
from utils.gcp import bq
import geopy.distance
import pandas as pd
import numpy as np
import logging

# BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
BQ_PREFIX = ''

PROJECT_NAME = os.environ['PROJECT_NAME']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/opt/airflow/gcp_credentials/GCS_BigQuery_write_cred.json'
CLIENT = bigquery.Client()

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['other', 'one_time']
)
def calc_distance_to_dim():
    @python_task
    def get_mrt_df():
        mrt_sql_query = """  
            SELECT mrt_station_id,lat,lng
            FROM `MRT_GCS_to_BQ_SRC_ODS_DIM.DIM_MRT_static_data`
        """  # noqa
        query_job = CLIENT.query(mrt_sql_query)
        df = query_job.to_dataframe()
        return df

    @python_task
    def get_bike_df():
        bike_df_sql_query = """  
            SELECT  bike_station_id,lat,lng 
            FROM `ETL_DIM.DIM_bike_station`
        """  # noqa
        query_job = CLIENT.query(bike_df_sql_query)
        df = query_job.to_dataframe()
        return df

    @python_task
    def get_bus_df():
        bus_sql_query = """  
            SELECT  bus_station_id,lat,lng
            FROM `BUS_GCS_to_BQ_SRC_ODS_DIM.DIM_Bus_static_data`
        """  # noqa
        query_job = CLIENT.query(bus_sql_query)
        df = query_job.to_dataframe()
        return df

    def calculate_distance_matrix(bike_df, second_df):
        bike_coords = bike_df[['lat', 'lng']].to_numpy()
        bus_coords = second_df[['lat', 'lng']].to_numpy()

        distances = np.zeros((bike_df.shape[0], second_df.shape[0]))

        for i, bike_coord in enumerate(bike_coords):
            for j, second_coord in enumerate(bus_coords):
                distances[i, j] = geopy.distance.geodesic(
                    bike_coord, second_coord).kilometers
        logging.info('distance calculation complete....')
        return distances

    @python_task
    def create_bike_bus_distance(bike_df: pd.DataFrame, bus_df: pd.DataFrame):
        distance_matrix = calculate_distance_matrix(bike_df, bus_df)

        distance_df = pd.DataFrame(
            distance_matrix, columns=bus_df['bus_station_id'])
        distance_df['bike_station_id'] = bike_df['bike_station_id']
        distance_df['lat'] = bike_df['lat']
        distance_df['lng'] = bike_df['lng']

        bike_bus_distance = distance_df.melt(id_vars=['bike_station_id', 'lat', 'lng'],
                                             var_name='bus_station_id', value_name='distance')

        bike_bus_distance = bike_bus_distance[[
            'bike_station_id', 'bus_station_id', 'distance']]
        return bike_bus_distance

    @python_task
    def create_bike_mrt_distance(bike_df: pd.DataFrame, mrt_df: pd.DataFrame):
        distance_matrix = calculate_distance_matrix(bike_df, mrt_df)

        distance_df = pd.DataFrame(
            distance_matrix, columns=mrt_df['mrt_station_id'])
        distance_df['bike_station_id'] = bike_df['bike_station_id']
        distance_df['lat'] = bike_df['lat']
        distance_df['lng'] = bike_df['lng']

        bike_mrt_distance = distance_df.melt(id_vars=['bike_station_id', 'lat', 'lng'],
                                             var_name='mrt_station_id', value_name='distance')

        bike_mrt_distance = bike_mrt_distance[[
            'bike_station_id', 'mrt_station_id', 'distance']]
        return bike_mrt_distance

    @python_task
    def upload_bike_bus_distance_to_bq(bike_bus_distance):
        youbike_bus_distance_schema = [
            bigquery.SchemaField("bike_station_id", "INT64"),
            bigquery.SchemaField("bus_station_id", "STRING"),
            bigquery.SchemaField("distance", "FLOAT")
        ]
        bq.upload_df_to_bq(client=CLIENT,
                           df=bike_bus_distance,
                           dataset_name="Harry_ETL_DIM",
                           table_name="DIM_youbike_bus_distance",
                           schema=youbike_bus_distance_schema,
                           filetype="csv",)

    @python_task
    def upload_bike_mrt_distance_to_bq(bike_mrt_distance):
        youbike_mrt_distance_schema = [
            bigquery.SchemaField("bike_station_id", "INT64"),
            bigquery.SchemaField("mrt_station_id", "STRING"),
            bigquery.SchemaField("distance", "FLOAT")
        ]
        bq.upload_df_to_bq(client=CLIENT,
                           df=bike_mrt_distance,
                           dataset_name="Harry_ETL_DIM",
                           table_name="DIM_youbike_mrt_distance",
                           schema=youbike_mrt_distance_schema,
                           filetype="csv",)

    bus_df = get_bus_df()
    mrt_df = get_mrt_df()
    bike_df = get_bike_df()
    bike_bus_distance_df = create_bike_bus_distance(
        bike_df=bike_df, bus_df=bus_df)
    upload_bike_bus_distance_to_bq(bike_bus_distance_df)
    bike_mrt_distance_df = create_bike_mrt_distance(
        bike_df=bike_df, mrt_df=mrt_df)
    upload_bike_mrt_distance_to_bq(bike_mrt_distance_df)


calc_distance_to_dim()
