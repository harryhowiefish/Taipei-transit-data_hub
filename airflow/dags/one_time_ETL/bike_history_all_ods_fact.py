'''
WORKING PROGRESS

This needs to be adjusted so it's triggered when
new history is ingested.

Also the query statement needs be changed to insert into

'''
# from airflow.decorators import dag, python_task
# from google.cloud import bigquery
# import os
# import pendulum
# import logging

# BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
# PROJECT_NAME = os.environ['PROJECT_NAME']
# CLIENT = bigquery.Client()

# default_args = {
#     'owner': 'TIR101_G2',
#     'retries': 0,
# }


# @dag(
#     default_args=default_args,
#     schedule='@once',
#     start_date=pendulum.today(tz='Asia/Taipei'),
#     tags=['one_time_ETL']
# )
# def bike_history_create_ods_fact():
#     src_dataset = f'{BQ_PREFIX}ETL_SRC'
#     ods_dataset = f'{BQ_PREFIX}ETL_ODS'
#     fact_dataset = f'{BQ_PREFIX}ETL_FACT'
#     src_name = 'src_bike_history'
#     ods_name = 'ods_bike_history'
#     fact_name = 'fact_bike_history'

#     @python_task
#     def src_to_ods():
#         job = CLIENT.query(
#             f"""CREATE OR REPLACE TABLE `{PROJECT_NAME}.{ods_dataset}.{ods_name}`
#                 PARTITION BY
#                     DATE_TRUNC(source_date, YEAR)
#                 OPTIONS (
#                     require_partition_filter = FALSE
#                 ) AS (
#                 SELECT
#                     rent_station,
#                     rent_time,
#                     return_station,
#                     return_time,
#                     rent,
#                     infodate as source_date,
#                     DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
#                     year,
#                     month,
#                 FROM `{PROJECT_NAME}.{src_dataset}.{src_name}`
#                 );
#             """  # noqa
#         )
#         while job.done() is False:
#             pass
#         logging.info(job.done(), job.exception())
#         if job.exception():
#             raise ConnectionRefusedError
#         return

#     @python_task
#     def ods_to_fact():
#         job = CLIENT.query(
#             f"""CREATE OR REPLACE TABLE `{PROJECT_NAME}.{fact_dataset}.{fact_name}`
#                 PARTITION BY
#                     DATE_TRUNC(source_date, YEAR)
#                 OPTIONS (
#                     require_partition_filter = FALSE
#                 ) AS (
#                 SELECT
#                     rent_station,
#                     EXTRACT(DATE FROM rent_time) as rent_date,
#                     EXTRACT(HOUR FROM rent_time) as rent_hour,
#                     return_station,
#                     EXTRACT(DATE FROM return_time) as return_date,
#                     EXTRACT(HOUR FROM return_time) as return_hour,
#                     CAST(split(rent ,':')[0] as integer)*60*60+CAST(split(rent ,':')[1] as integer)*60+CAST(split(rent ,':')[2] as integer) as usage_time,
#                     source_date,
#                     create_time,
#                     year,
#                     month
#                 FROM `{PROJECT_NAME}.{ods_dataset}.{ods_name}`
#                 );
#                 """  # noqa
#         )
#         while job.done() is False:
#             pass
#         logging.info(job.done(), job.exception())
#         if job.exception():
#             raise ConnectionRefusedError
#         return
#     src_to_ods() >> ods_to_fact()


# bike_history_create_ods_fact()
