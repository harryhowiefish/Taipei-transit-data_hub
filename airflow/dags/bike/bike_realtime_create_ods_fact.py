
from airflow.decorators import dag, python_task
from google.cloud import bigquery
import os
import pendulum
import logging

BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# BQ_PREFIX = ''
# BUCKET_TYPE = os.environ['BUCKET_TYPE']
BUCKET_TYPE = ''

PROJECT_NAME = os.environ['PROJECT_NAME']
CLIENT = bigquery.Client()

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='@once',
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['bike', 'one_time']
)
def bike_realtime_create_ods_fact():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    src_name_before_0503 = 'SRC_bike_realtime_before_0503'
    src_name_0503 = 'SRC_bike_realtime_0503'
    src_name_after_0503 = 'SRC_bike_realtime_after_0503'
    ods_dataset = f'{BQ_PREFIX}ETL_ODS'
    ods_name = 'ODS_bike_realtime'
    fact_dataset = f'{BQ_PREFIX}ETL_FACT'
    fact_name = 'FACT_bike_realtime'
    dim_dataset = f'{BQ_PREFIX}ETL_DIM'
    dim_name = 'DIM_bike_station'
    mapping_name = 'ODS_bike_station_mapping'

    @python_task
    def create_ods_from_three_src():

        job = CLIENT.query(
            f"""
            CREATE OR REPLACE TABLE `{ods_dataset}.{ods_name}`
            PARTITION BY
                    DATE_TRUNC(create_time, DAY)
            OPTIONS (
                require_partition_filter = FALSE
            ) AS (
                (
                    SELECT
                        sno AS bike_station_id,
                        split(sna,'_')[1] AS station_name,
                        'TPE' as city_code,
                        sarea AS district,
                        ar AS address,
                        act,
                        DATETIME(timestamp_sub(srcUpdateTime,INTERVAL 8 HOUR), 'Asia/Taipei') AS source_time,
                        DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
                        CAST(tot as INTEGER) AS total_space,
                        CAST(sbi as INTEGER) AS aval_bike,
                        lat,
                        lng,
                        CAST(bemp as INTEGER) AS aval_space
                    FROM `{src_dataset}.{src_name_before_0503}`
                )
                UNION ALL
                (
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
                    FROM `{src_dataset}.{src_name_after_0503}`)

                UNION ALL
                (
                    SELECT
                        sno AS bike_station_id,
                        split(sna,'_')[1] AS station_name,
                        'TPE' as city_code,
                        sarea AS district,
                        ar AS address,
                        act,
                        DATETIME(timestamp_sub(srcUpdateTime,INTERVAL 8 HOUR), 'Asia/Taipei') AS source_time,
                        DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
                        CASE
                            WHEN total is not null then CAST(total as INTEGER)
                            WHEN tot is not null then CAST(tot as INTEGER)
                        END
                        AS total_space,
                        CASE
                            WHEN available_rent_bikes is not null then CAST(available_rent_bikes as INTEGER)
                            WHEN sbi is not null then CAST(sbi as INTEGER)
                        END
                        AS aval_bike,
                        CASE
                            WHEN latitude is not null then CAST(latitude as INTEGER)
                            WHEN lat is not null then CAST(lat as INTEGER)
                        END
                        AS lat,
                        CASE
                            WHEN longitude is not null then CAST(longitude as INTEGER)
                            WHEN lng is not null then CAST(lng as INTEGER)
                        END
                        AS lng,
                        CASE
                            WHEN available_return_bikes is not null then CAST(available_return_bikes as INTEGER)
                            WHEN bemp is not null then CAST(bemp as INTEGER)
                        END
                        AS aval_space
                    FROM `{src_dataset}.{src_name_0503}`)
        )
        ;
    """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def ods_to_dim():
        job = CLIENT.query(
            f"""
            CREATE OR REPLACE TABLE `{dim_dataset}.{dim_name}` AS
            SELECT 
                bike_station_id,
                t2.station_name as station_name,
                total_space,
                lat,
                lng,
                district,
                address,
                act,
                create_time,
                source_time 
            FROM
                (SELECT *
                FROM 
                    (SELECT 
                        * ,
                        ROW_NUMBER() OVER (PARTITION by `bike_station_id` ORDER BY `source_time` ASC) AS `row_num`
                    FROM `{ods_dataset}.{ods_name}`
                    ) AS inner_t
                WHERE inner_t.row_num=1) as t1
            INNER JOIN `{PROJECT_NAME}.{ods_dataset}.{mapping_name}` t2
                ON t1.station_name = t2.open_data_station_name
            ;
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def ods_to_fact():
        job = CLIENT.query(
            f"""
            CREATE OR REPLACE TABLE `{fact_dataset}.{fact_name}`
            PARTITION BY
                    DATE_TRUNC(source_time, MONTH)
                OPTIONS (
                    require_partition_filter = FALSE
                ) AS
            SELECT 
                bike_station_id,
                t2.station_name as station_name,
                aval_bike,
                aval_space,
                create_time,
                source_time 
            FROM `{ods_dataset}.{ods_name}` t1
            INNER JOIN `{PROJECT_NAME}.{ods_dataset}.{mapping_name}` t2
            ON t1.station_name = t2.open_data_station_name;
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return
    create_ods_from_three_src() >> [ods_to_dim(), ods_to_fact()]


bike_realtime_create_ods_fact()
