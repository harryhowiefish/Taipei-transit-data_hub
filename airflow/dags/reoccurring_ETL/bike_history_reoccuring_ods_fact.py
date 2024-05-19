
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
#     tags=['reoccurring_ETL']
# )
# def bike_history_src_to_ods():

#     src_name = 'src_bike_history'

#     @python_task
#     def gcs_to_src():
#         job = CLIENT.query(
#             f"""
#                 CREATE OR REPLACE EXTERNAL TABLE {PROJECT_NAME}.{BQ_PREFIX}ETL_SRC.{src_name}
#                 (
#                     index INT,
#                     rent_time TIMESTAMP,
#                     rent_station STRING,
#                     return_time TIMESTAMP,
#                     return_station STRING,
#                     rent TIME,
#                     infodate DATE
#                 )
#                 WITH PARTITION COLUMNS
#                 OPTIONS (
#                     format = 'CSV',
#                 uris = [
#                         'gs://static_reference/bike_history/*.csv'
#                     ],
#                     hive_partition_uri_prefix = 'gs://static_reference/bike_history/',
#                     skip_leading_rows = 1,
#                     max_bad_records = 1
#                 )
#             """  # noqa
#         )
#         while job.done() is False:
#             pass
#         logging.info(job.done(), job.exception())
#         if job.exception():
#             raise ConnectionRefusedError
#         return
#     gcs_to_src()


# bike_history_gcs_to_src()
