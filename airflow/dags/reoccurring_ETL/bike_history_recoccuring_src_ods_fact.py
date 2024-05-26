
from airflow.decorators import dag, python_task, branch_python
from google.cloud import bigquery
import os
import pendulum
import logging
from google.cloud.exceptions import NotFound


BQ_PREFIX = os.environ['BIGQUERY_PREFIX']
PROJECT_NAME = os.environ['PROJECT_NAME']
CLIENT = bigquery.Client()

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.today(tz='Asia/Taipei'),
    tags=['bike', 'reocurring']
)
def bike_history_recoccuring_src_ods_fact():
    src_dataset = f'{BQ_PREFIX}ETL_SRC'
    ods_dataset = f'{BQ_PREFIX}ETL_ODS'
    fact_dataset = f'{BQ_PREFIX}ETL_FACT'
    src_name = 'SRC_bike_history'
    ods_name = 'ODS_bike_history'
    fact_name = 'FACT_bike_history'

    @branch_python.branch_task
    def check_if_bike_history_fact_exist():
        dataset = CLIENT.dataset(fact_dataset)
        table_id = dataset.table(fact_name)
        try:
            # Try to get the table
            CLIENT.get_table(table_id)
            return 'get_latest_date_in_fact'
        except NotFound:
            return 'create_ods_from_src'

    @python_task
    def create_ods_from_src():
        job = CLIENT.query(
            f"""CREATE OR REPLACE TABLE `{PROJECT_NAME}.{ods_dataset}.{ods_name}`
                PARTITION BY
                    DATE_TRUNC(source_date, YEAR)
                OPTIONS (
                    require_partition_filter = FALSE
                ) AS (
                SELECT
                    rent_station,
                    rent_time,
                    return_station,
                    return_time,
                    rent,
                    infodate as source_date,
                    DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
                    year,
                    month,
                FROM `{PROJECT_NAME}.{src_dataset}.{src_name}`
                );
            """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def create_fact_from_ods():
        job = CLIENT.query(
            f"""CREATE OR REPLACE TABLE `{PROJECT_NAME}.{fact_dataset}.{fact_name}`
                PARTITION BY
                    DATE_TRUNC(source_date, YEAR)
                OPTIONS (
                    require_partition_filter = FALSE
                ) AS (
                SELECT
                    rent_station,
                    EXTRACT(DATE FROM rent_time) as rent_date,
                    EXTRACT(HOUR FROM rent_time) as rent_hour,
                    return_station,
                    EXTRACT(DATE FROM return_time) as return_date,
                    EXTRACT(HOUR FROM return_time) as return_hour,
                    (CAST(split(rent ,':')[0] as integer)*60*60+
                    CAST(split(rent ,':')[1] as integer)*60+
                    CAST(split(rent ,':')[2] as integer)) as usage_time,
                    source_date,
                    create_time,
                    year,
                    month
                FROM `{PROJECT_NAME}.{ods_dataset}.{ods_name}`
                );
                """  # noqa
        )
        while job.done() is False:
            pass
        logging.info(job.done(), job.exception())
        if job.exception():
            raise ConnectionRefusedError
        return

    @python_task
    def get_latest_date_in_fact():
        job = CLIENT.query(
            f'''
            SELECT max(source_date) FROM {PROJECT_NAME}.{fact_dataset}.{fact_name} LIMIT 1
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
                rent_station,
                rent_time,
                return_station,
                return_time,
                rent,
                infodate as source_date,
                DATETIME(CURRENT_TIMESTAMP(), 'Asia/Taipei')  as create_time,
                year,
                month,
            FROM
                {PROJECT_NAME}.{src_dataset}.{src_name}
            where year > extract(year from DATE('{date}')) or 
            (year = extract(year from DATE('{date}')) and month > extract(month from DATE('{date}')))
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
                rent_station,
                EXTRACT(DATE FROM rent_time) as rent_date,
                EXTRACT(HOUR FROM rent_time) as rent_hour,
                return_station,
                EXTRACT(DATE FROM return_time) as return_date,
                EXTRACT(HOUR FROM return_time) as return_hour,
                (CAST(split(rent ,':')[0] as integer)*60*60+
                CAST(split(rent ,':')[1] as integer)*60+
                CAST(split(rent ,':')[2] as integer)) as usage_time,
                source_date,
                create_time,
                year,
                month
            FROM
                {PROJECT_NAME}.{ods_dataset}.{ods_name}
            where source_date > DATE('{date}')
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
    ods_from_src = create_ods_from_src()
    check_if_bike_history_fact_exist() >> [ods_from_src, last_date]
    ods_from_src >> create_fact_from_ods()
    src_to_ods(last_date) >> ods_to_dim(last_date)


bike_history_recoccuring_src_ods_fact()
