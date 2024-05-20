from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import logging
import pendulum

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
PROJECT_NAME = os.environ['PROJECT_NAME']
CLIENT = bigquery.Client()


# these are some common arguments for dags
default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(default_args=default_args, schedule='@once',
     start_date=pendulum.today(tz='Asia/Taipei'), tags=['MART'])
def mart_pipeline_2():

    @python_task
    def build_mart():

        mart_name = 'MART_pipeline2'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE
            `{PROJECT_NAME}.{BQ_PREFIX}ETL_MART.{mart_name}` AS (
            SELECT
                rent_station,
                district,
                lat,
                lng,
                year,
                month,
                weekend,
                rent_hour,
                traffic_count
            FROM (
                SELECT
                rent_station,
                t1.year,
                t1.month,
                rent_hour,
                weekend,
                COUNT(*) AS traffic_count
                FROM
                `tir101-group2-422603.ETL_FACT.FACT_bike_history` t1
                INNER JOIN (
                SELECT
                    `date`,
                    weekend,
                    year,
                    month
                FROM
                    `tir101-group2-422603.ETL_DIM.DIM_time_table`) t2
                ON
                t1.rent_date=t2.date
                -- WHERE EXTRACT(YEAR FROM source_date) = 2024
                GROUP BY
                rent_station,
                t1.year,
                t1.month,
                weekend,
                rent_hour ) t1
            INNER JOIN (
                SELECT
                station_name,
                lat,
                lng,
                district,
                disable
                FROM
                `tir101-group2-422603.ETL_DIM.DIM_bike_station`
                WHERE
                disable = 1) AS t3
            ON
                t1.rent_station = SPLIT(t3.station_name,'_')[1]
    );
            '''  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return
    build_mart()


mart_pipeline_2()
