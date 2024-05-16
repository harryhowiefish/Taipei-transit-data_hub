
import os
from gcp_utils import bq_utils
from google.cloud import bigquery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp_credentials/GCS_BigQuery_write_cred.json'


def delete_table():
    pass


def get_bike_one_day_and_save_csv(str_date: str):
    '''
    str_date example 05_08_2024
    '''
    client = bigquery.Client()
    query = '''
        SELECT * FROM `tir101-group2-422603.Youbike_ODS.youbike_src_after0504`
        WHERE dt='{}'
        '''.format(str_date)
    print(query)
    df = bq_utils.query_bq_to_df(client, query)
    df.to_csv(f'{str_date}.csv', index=False)


get_bike_one_day_and_save_csv('05_08_2024')
