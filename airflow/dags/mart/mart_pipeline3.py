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
def mart_pipeline_3():

    @python_task
    def build_mart():

        mart_name = 'MART_pipeline3'
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
                bike_history AS (
                SELECT
                rent_station,
                rent_date,
                rent_hour,
                weekend,
                COUNT(*) AS traffic_count
                FROM
                `tir101-group2-422603.ETL_FACT.FACT_bike_history` t1
                INNER JOIN (
                SELECT
                    `date`,
                    weekend,
                FROM
                    `tir101-group2-422603.ETL_DIM.DIM_time_table`) t2
                ON
                t1.rent_date=t2.date
                -- WHERE EXTRACT(YEAR FROM source_date) = 2024
                GROUP BY
                rent_station,
                rent_date,
                weekend,
                rent_hour )
                -- , 'district', 'near_to_bus', 'near_to_mrt', 'lend_date', 'lend_hour', 'traffic_count','weekend'
            SELECT
                bike_station_id,
                rent_station AS station_name,
                district,
                dist_to_mrt,
                dist_to_bus,
                rent_date,
                rent_hour,
                traffic_count,
                weekend
            FROM
                bike_history t1
            INNER JOIN
                `tir101-group2-422603.ETL_DIM.DIM_bike_station` t2
            ON
                t1.rent_station = SPLIT(t2.station_name,'_')[1]
            LEFT JOIN
                nearest_mrt t3
            USING
                (bike_station_id)
            LEFT JOIN
                nearest_bus t4
            USING
                (bike_station_id)
            WHERE
                t2.disable=1 
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


mart_pipeline_3()
