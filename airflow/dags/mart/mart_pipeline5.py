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


@dag(default_args=default_args, schedule='@daily',
     start_date=pendulum.today(tz='Asia/Taipei'),
     tags=['other', 'reocurring'])
def mart_pipeline_5():

    @python_task
    def build_mart():

        mart_name = 'MART_pipeline5'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE
            `{PROJECT_NAME}.{BQ_PREFIX}ETL_MART.{mart_name}` AS (
            WITH
                near_mrt_bike_stations AS (
                SELECT
                DISTINCT bike_station_id,
                bike_station_name,
                station_name AS mrt_station_name
                FROM (
                SELECT
                    *
                FROM
                    `tir101-group2-422603.ETL_DIM.DIM_youbike_mrt_distance`
                WHERE
                    distance<0.3 ) t1
                INNER JOIN (
                SELECT
                    bike_station_id,
                    SPLIT(station_name,'_')[1] AS bike_station_name
                FROM
                    `tir101-group2-422603.ETL_DIM.DIM_bike_station`
                WHERE
                    disable = 1) AS t2
                USING
                (bike_station_id)
                INNER JOIN (
                SELECT
                    mrt_station_id,
                    station_name
                FROM
                    `tir101-group2-422603.MRT_GCS_to_BQ_SRC_ODS_DIM.DIM_MRT_static_data` )
                USING
                (mrt_station_id)),
                realtime_risk AS (
                SELECT
                bike_station_id,
                EXTRACT(DATE
                FROM
                    source_time) AS `date`,
                EXTRACT(HOUR
                FROM
                    source_time) AS `hour`,
                CAST(aval_bike<LEAST(total_space * 0.1, 5) AS integer) AS almost_empty,
                FROM (
                SELECT
                    bike_station_id,
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
                USING
                (bike_station_id))
            SELECT
                *
            FROM
                near_mrt_bike_stations
            INNER JOIN (
                SELECT
                bike_station_id,
                `date`,
                `hour`,
                AVG(almost_empty) AS hourly_empty_risk
                FROM
                realtime_risk
                GROUP BY
                bike_station_id,
                `date`,
                `hour`)
            USING
                (bike_station_id) 
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


mart_pipeline_5()
