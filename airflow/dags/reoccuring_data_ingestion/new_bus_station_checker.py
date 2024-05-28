import pandas as pd
from google.cloud import storage, bigquery
from airflow.decorators import dag, python_task, branch_task
from utils.gcp import gcs, bq
import os
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import tdx
import requests
import logging
import json


BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
BUCKET_TYPE = os.environ['BUCKET_TYPE']
# BUCKET_TYPE = ''

PROJECT_NAME = os.environ['PROJECT_NAME']
GCS_CLIENT = storage.Client()
default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['bus', 'reoccuring'],
)
def new_bus_station_checker():

    client = tdx.TDX(os.environ['TDX_APP_ID'], os.environ['TDX_APP_KEY'])

    @python_task
    def download_to_df():
        data = client.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/Station/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)
        return df

    @python_task
    def get_station_id_from_dim():
        bq_client = bigquery.Client()
        query = f'''SELECT bus_station_id FROM `{PROJECT_NAME}.{BQ_PREFIX}ETL_DIM.DIM_bus_station` '''  # noqa
        df = bq.query_bq_to_df(bq_client, query)
        df['bus_station_id'] = df['bus_station_id'].astype('str')
        return df

    @python_task
    def compare_dfs(new_df, old_df):
        result_df = new_df[~new_df['StationID'].isin(old_df['bus_station_id'])]
        return result_df

    @branch_task
    def df_len(df):
        if len(df) == 0:
            return
        else:
            return 'save_to_gcs'

    @python_task
    def save_to_gcs(df):
        gcs.upload_df_to_gcs(GCS_CLIENT,
                             bucket_name=f'{BUCKET_TYPE}static_reference',
                             blob_name=f"bus/tpe_station/{pendulum.today().format('MM_DD_YYYY')}.csv",  # noqa
                             df=df)

    @python_task
    def geocoding(df: pd.DataFrame):
        df.rename(
            {
                'StationPosition.PositionLat': 'lat',
                'StationPosition.PositionLon': 'lng'
            }, axis=1, inplace=True
        )
        data = df[['StationID', 'StationAddress', 'lat', 'lng']
                  ].to_dict('records')
        cloud_function_url = 'https://asia-east1-tir101-group2-422603.cloudfunctions.net/get_district'
        bucket = GCS_CLIENT.bucket(f'{BUCKET_TYPE}static_reference')
        for item in data:
            response = requests.post(cloud_function_url, json=item)
            if response.status_code == 200:
                blob = bucket.blob(f"bus/bus_geocoding_json/{item['StationID']}.json")  # noqa
                result = json.loads(response.text)
                result['bus_station_id'] = item['StationID']
                result['address'] = item['StationAddress']
                logging.info(result)
                blob.upload_from_string(json.dumps(result, ensure_ascii=False))
                logging.info(f"{blob.name} created!")
            else:
                raise ConnectionError(response.status_code)
        return

    trigger = TriggerDagRunOperator(
        task_id="trigger_etl",
        # Ensure this equals the dag_id of the DAG to trigger
        trigger_dag_id="bus_station_src_ods_dim",
    )

    new_df = download_to_df()
    old_df = get_station_id_from_dim()
    filtered_df = compare_dfs(new_df, old_df)
    df_len(filtered_df) >> [save_to_gcs(
        filtered_df)] >> geocoding(filtered_df) >> trigger


new_bus_station_checker()
