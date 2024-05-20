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
def mart_pipeline_4():

    @python_task
    def build_mart():

        mart_name = 'MART_pipeline4'
        job = CLIENT.query(
            f'''
            CREATE OR REPLACE TABLE
            `{PROJECT_NAME}.{BQ_PREFIX}ETL_MART.{mart_name}` AS (
            WITH
            nearest_mrt AS (
            SELECT
                bike_station_id,
                mrt_station_id,
                distance AS dist_to_mrt
            FROM (
                SELECT
                bike_station_id,
                mrt_station_id,
                distance,
                ROW_NUMBER() OVER (PARTITION BY bike_station_id ORDER BY distance) AS rn
                FROM
                `tir101-group2-422603.ETL_DIM.DIM_youbike_mrt_distance` )
            WHERE
                rn = 1),
            nearest_bus AS (
            SELECT
                bike_station_id,
                bus_station_id,
                distance AS dist_to_bus
            FROM (
                SELECT
                bike_station_id,
                bus_station_id,
                distance,
                ROW_NUMBER() OVER (PARTITION BY bike_station_id ORDER BY distance) AS rn
                FROM
                `tir101-group2-422603.ETL_DIM.DIM_youbike_bus_distance` )
            WHERE
                rn = 1),
            realtime_risk AS (
            SELECT
                bike_station_id,
                station_name,
                lng,
                lat,
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
                station_name,
                lng,
                lat,
                total_space,
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
            t1.station_name,
            t2.dist_to_mrt,
            t3.dist_to_bus,
            t1.date,
            t1.hour,
            t1.lng,
            t1.lat,
            t1.hourly_empty_risk
            FROM (
            SELECT
                bike_station_id,
                ANY_VALUE(station_name) AS station_name,
                ANY_VALUE(lng) AS lng,
                ANY_VALUE(lat) AS lat,
                `date`,
                `hour`,
                AVG(almost_empty) AS hourly_empty_risk
            FROM
                realtime_risk
            GROUP BY
                bike_station_id,
                `date`,
                `hour`) t1
            LEFT JOIN
            nearest_mrt t2
            USING
            (bike_station_id)
            LEFT JOIN
            nearest_bus t3
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


mart_pipeline_4()
