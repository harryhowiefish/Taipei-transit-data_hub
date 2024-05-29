import pandas as pd
from google.cloud import storage, bigquery
from airflow.decorators import dag, python_task, branch_task
from utils.gcp import gcs, bq
import os
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
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

app_id = os.getenv("TDX_APP_ID")
app_key = os.getenv("TDX_APP_KEY")


class Auth():

    def __init__(self, app_id, app_key):
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        content_type = 'application/x-www-form-urlencoded'
        grant_type = 'client_credentials'

        return {
            'content-type': content_type,
            'grant_type': grant_type,
            'client_id': self.app_id,
            'client_secret': self.app_key
        }


class data():

    def __init__(self, app_id, app_key, auth_response):
        self.app_id = app_id
        self.app_key = app_key
        self.auth_response = auth_response

    def get_data_header(self):
        if self.auth_response == "":
            access_token = ""
        else:
            auth_JSON = json.loads(self.auth_response.text)
            access_token = auth_JSON.get('access_token')

        return {
            'authorization': 'Bearer ' + access_token,
            'Accept-Encoding': 'gzip'
        }


@dag(
    default_args=default_args,
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['mrt', 'reoccuring'],
)
def new_mrt_station_checker():
    @python_task
    def download_to_df():
        auth_url = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"  # noqa
        url = 'https://tdx.transportdata.tw/api/basic/v2/Rail/Metro/Station/TRTC?format=JSON'  # noqa
        a = Auth(app_id, app_key)
        auth_response = requests.post(auth_url, a.get_auth_header())
        d = data(app_id, app_key, auth_response)
        response = requests.get(url, headers=d.get_data_header())
        return (pd.json_normalize(json.loads(response.text), max_level=1))

    @python_task
    def get_station_id_from_dim():
        bq_client = bigquery.Client()
        query = f'''SELECT mrt_station_id FROM `{PROJECT_NAME}.{BQ_PREFIX}ETL_DIM.DIM_mrt_station` '''  # noqa
        df = bq.query_bq_to_df(bq_client, query)
        return df

    @python_task
    def compare_dfs(new_df, old_df):
        result_df = new_df[~new_df['StationID'].isin(old_df['mrt_station_id'])]
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
                             blob_name=f"mrt/trtc_station/{pendulum.today().format('MM_DD_YYYY')}.csv",  # noqa
                             df=df)

    trigger = TriggerDagRunOperator(
        task_id="trigger_etl",
        # Ensure this equals the dag_id of the DAG to trigger
        trigger_dag_id="mrt_station_src_ods_dim",
    )

    new_df = download_to_df()
    old_df = get_station_id_from_dim()
    difference_df = compare_dfs(new_df, old_df)
    df_len(difference_df) >> save_to_gcs(df=difference_df) >> trigger


new_mrt_station_checker()
