from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import requests


def get_rt_data():

    client = MongoClient('mongodb://admin:example@mongo:27017')
    ubike = client['ubike']
    ubike_rt = ubike['ubike_rt']
    resp = requests.get(
        'https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json')  # noqa
    single_result = resp.json()
    ubike_rt.insert_many(single_result)
    print(ubike_rt.count_documents({}))


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

pipeline = DAG(
    dag_id='crawler_with_pymongo',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 4, 10),
    catchup=False
)

start_task = PythonOperator(
    python_callable=get_rt_data,
    task_id='get_rt_data',
    dag=pipeline,


)
