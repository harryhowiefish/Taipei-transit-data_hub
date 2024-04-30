from airflow import DAG
from datetime import timedelta, datetime
import requests
from airflow.operators.python import PythonOperator

from airflow.providers.mongo.hooks.mongo import MongoHook


def get_rt_data():
    mongo_hook = MongoHook(conn_id='mongodb')
    resp = requests.get(
        'https://tcgbusfs.blob.core.windows.net/dotapp/youbike/v2/youbike_immediate.json')  # noqa
    single_result = resp.json()
    mongo_hook.insert_many('ubike_rt', single_result, 'ubike')
    all_data = mongo_hook.find(
        mongo_collection='ubike_rt', query={}, mongo_db='ubike')
    print(all_data.collection.count_documents({}))


default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

pipeline = DAG(
    dag_id='crawler_with_mongohook',
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
