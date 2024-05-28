import pandas as pd
from google.cloud import storage
from airflow.decorators import dag, python_task
from utils.gcp import gcs
import os
import pendulum
from utils.etl import tdx

BUCKET_TYPE = os.environ['BUCKET_TYPE']


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['bus', 'one_time'],
)
def bus_station_to_gcs():

    client = tdx.TDX(os.environ['TDX_APP_ID'], os.environ['TDX_APP_KEY'])

    @python_task
    def download_to_df():
        data = client.get_response(
            "https://tdx.transportdata.tw/api/basic/v2/Bus/Station/City/Taipei?%24format=JSON")
        df = pd.json_normalize(data)
        return df

    @python_task
    def save_to_gcs(df) -> None:
        client = storage.Client()
        gcs.upload_df_to_gcs(client=client,
                             bucket_name=f'{BUCKET_TYPE}static_reference',
                             blob_name="bus/tpe_station/tpe_bus_station.csv",
                             df=df)
        return

    # an additional task to check geocoding completion can be added here

    df = download_to_df()
    save_to_gcs(df)


bus_station_to_gcs()
