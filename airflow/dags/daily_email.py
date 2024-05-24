from airflow.decorators import dag, python_task
from airflow.operators.email import EmailOperator
from google.cloud import storage
import pendulum
import pandas as pd
from io import StringIO
from pendulum.datetime import DateTime

CLIENT = storage.Client()

default_args = {
    'owner': 'TIR101_G2',
    'retries': 0,
}


@dag(
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2024, 3, 17, tz='Asia/Taipei'),
    tags=['other'],
    catchup=True
)
def daily_ingestion_summary():
    @python_task
    def print_execution_date(execution_date=None):
        print(f"The logical date of this DAG run is: {execution_date}")

    @python_task
    def create_email_content(execution_date: DateTime = None):
        bucket = CLIENT.bucket('youbike_realtime')
        date = execution_date.subtract(days=1).format('MM_DD_YYYY')
        prefix = f'v2_data/dt={date}'
        blob_list = [blob for blob in bucket.list_blobs(prefix=prefix)]
        ingested_files = {item.name.split('/')[-1]: item for item in blob_list}
        missing_time = []
        last_time = None
        last_row_count = None
        station_num_change = dict()
        for hour in range(0, 24):
            for min in range(0, 60, 10):
                filename = f"{str(hour).zfill(2)}_{str(min).zfill(2)}.csv"
                time_string = filename.split('.')[0].replace('_', ':')
                if filename not in ingested_files.keys():
                    missing_time.append(time_string)
                    continue
                row_count = pd.read_csv(
                    StringIO(ingested_files[filename].download_as_text())).shape[0]
                if last_row_count is None:
                    last_row_count = row_count
                    last_time = time_string
                    continue
                if row_count != last_row_count:
                    station_num_change[last_time] = last_row_count
                    station_num_change[time_string] = row_count
                last_row_count = row_count
                last_time = time_string

        msg = ''
        if missing_time:
            msg += f"<p>missing data from the following time: {', '.join(missing_time)}. </p>"  # noqa
        else:
            msg += "<p>All data successfully ingested! All is well... </p>"
        print(station_num_change)
        if len(station_num_change) == 0:
            msg += f'<p>The number of stations is consistent throughout the day at {last_row_count} stations \n </p>'  # noqa
        else:
            msg += '<p>WARNING! The number of stations changed during the day. </p>'
            timestamps = sorted(station_num_change.keys())
            for idx in range(len(timestamps) - 1):
                before_time = timestamps[idx]
                after_time = timestamps[idx + 1]
                before_station_num = station_num_change[before_time]
                after_station_num = station_num_change[after_time]
                msg += f'<p>Number of station changed from {before_station_num} to {after_station_num} between {before_time}~{after_time} </p>'  # noqa
        return msg

    @python_task
    def send_email(msg, execution_date: DateTime = None):
        # Pull the string from XComs
        date = execution_date.subtract(days=1).format('MM_DD_YYYY')
        # EmailOperator task
        email_task = EmailOperator(
            task_id='send_email',
            to='harryhowiefish@gmail.com',
            subject=f'Daily summary from {date}',
            html_content=msg,
        )
        return email_task.execute(context=None)

    first_task = print_execution_date()

    msg = create_email_content()
    first_task >> msg
    send_email(msg)


daily_ingestion_summary()
