from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import logging
import pendulum

# BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
BQ_PREFIX = ''
PROJECT_NAME = os.environ['PROJECT_NAME']
CLIENT = bigquery.Client()


# these are some common arguments for dags
default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(default_args=default_args, schedule='@once',
     start_date=pendulum.today(tz='Asia/Taipei'), tags=['MART'])
def mart_pipeline_1():

    @python_task
    def build_mart():

        mart_name = 'MART_pipeline1'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE
            `{PROJECT_NAME}.{BQ_PREFIX}ETL_MART.{mart_name}` AS (
            WITH
                realtime_risk AS (
                SELECT
                *,
                LEAST(total_space * 0.1, 5) AS thres,
                CAST(aval_bike<LEAST(total_space * 0.1, 5) AS integer) AS almost_empty,
                CAST(aval_space<LEAST(total_space * 0.1, 5) AS integer) AS almost_full
                FROM (
                SELECT
                    bike_station_id,
                    station_name,
                    total_space
                FROM
                    `tir101-group2-422603.ETL_DIM.DIM_bike_station`
                WHERE
                    disable=1) AS t1
                INNER JOIN (
                SELECT
                    bike_station_id,
                    aval_bike,
                    aval_space,
                    source_time
                FROM
                    `tir101-group2-422603.ETL_FACT.FACT_bike_realtime` )AS t2
                ON
                t1.bike_station_id = t2.bike_station_id)
            SELECT
                *
            FROM (
                SELECT
                TIMESTAMP_TRUNC(source_time, HOUR) AS truncated_timestamp,
                AVG(almost_empty) AS avg_almost_empty,
                AVG(almost_full) AS avg_almost_full
                FROM
                realtime_risk
                GROUP BY
                truncated_timestamp ) t1
            INNER JOIN (
                SELECT
                date,
                day_of_week,
                weekend
                FROM
                `tir101-group2-422603.ETL_DIM.DIM_time_table`) t2
            ON
                DATE(t1.truncated_timestamp)= t2.date)
            '''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return
    build_mart()


mart_pipeline_1()
