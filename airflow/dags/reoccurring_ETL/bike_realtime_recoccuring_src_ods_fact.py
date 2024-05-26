
from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
import logging


BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
PROJECT_NAME = os.environ['PROJECT_NAME']
CLIENT = bigquery.Client()

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='@daily',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['bike', 'reocurring']
)
def bike_realtime_recoccuring_src_ods_fact():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    ods_dataset = f'{BQ_PREFIX}ETL_ODS'
    fact_dataset = f'{BQ_PREFIX}ETL_FACT'
    src_name = 'SRC_bike_realtime_after_0503'
    ods_name = 'ODS_bike_realtime'
    fact_name = 'FACT_bike_realtime'
    mapping_name = 'ODS_bike_station_mapping'

    @python_task
    def get_latest_date_in_fact():
        job = CLIENT.query(
            f'''
            SELECT FORMAT_DATE('%m_%d_%Y',TIMESTAMP_ADD(max(source_time), INTERVAL 1 DAY)) FROM {PROJECT_NAME}.{fact_dataset}.{fact_name} LIMIT 1
            '''  # noqa
        )
        results = job.result()
        first_row = next(results)
        first_value = first_row.values()[0]
        return first_value

    @python_task
    def src_to_ods(date):
        job = CLIENT.query(
            f'''
            INSERT INTO `{PROJECT_NAME}.{ods_dataset}.{ods_name}` (
            SELECT 
                sno AS bike_station_id,
                split(sna,'_')[1] AS station_name,
                'TPE' as city_code,
                sarea AS district,
                ar AS address,
                act,
                DATETIME(timestamp_sub(srcUpdateTime,INTERVAL 8 HOUR), 'Asia/Taipei') AS source_time,
                DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
                CAST(total as INTEGER) AS total_space,
                CAST(available_rent_bikes as INTEGER) AS aval_bike,
                latitude AS lat,
                longitude AS lng,
                CAST(available_return_bikes as INTEGER) AS aval_space
            FROM
                {PROJECT_NAME}.{src_dataset}.{src_name}
            WHERE dt='{date}'
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        rows_inserted = job.num_dml_affected_rows
        logging.info(f"Number of rows inserted: {rows_inserted}")
        return

    @python_task
    def ods_to_dim(date):
        job = CLIENT.query(
            f'''
            INSERT INTO `{PROJECT_NAME}.{fact_dataset}.{fact_name}` (
            SELECT
                bike_station_id,
                t2.station_name as station_name,
                aval_bike,
                aval_space,
                create_time,
                source_time 
            FROM
                (SELECT *
                   FROM {PROJECT_NAME}.{ods_dataset}.{ods_name}
            WHERE date(source_time) = PARSE_DATE('%m_%d_%Y', '{date}')) as t1
            INNER JOIN `{PROJECT_NAME}.{ods_dataset}.{mapping_name}` as t2
            ON t1.station_name = t2.open_data_station_name
            )'''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        rows_inserted = job.num_dml_affected_rows
        logging.info(f"Number of rows inserted: {rows_inserted}")
        return

    last_date = get_latest_date_in_fact()
    src_to_ods(last_date) >> ods_to_dim(last_date)


bike_realtime_recoccuring_src_ods_fact()
