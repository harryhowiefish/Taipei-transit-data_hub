from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
# import requests
import pandas as pd
# import json
# from dotenv import load_dotenv
# import re
from sqlalchemy import create_engine
# import pymysql
# 設定資料庫連線資訊

# host = 'host.docker.internal'
# port = 3306
# username_sql = os.getenv("ANDY_USERNAME_SQL")
# password_sql = os.getenv("ANDY_PASSWORD_SQL")
# db = 'group2_db'
# charset = 'utf8mb4'


def task1():
    print("Running Task 1")


def task2():
    # conn = pymysql.connect(host=host, port=port, user=username_sql, passwd=password_sql, db=db, charset=charset)
    # cursor = conn.cursor()
    # print('Successfully connected!')
    # sql_scripts="""
    # CREATE TABLE `dockertest`(
    #     `id` int primary key,
    #     `name` varchar(10) not null
    # );
    # """
    # cursor.execute(sql_scripts)
    # conn.commit()
    # cursor.close()
    # conn.close()
    username_sql = os.getenv("ANDY_USERNAME_SQL")
    password_sql = os.getenv("ANDY_PASSWORD_SQL")
    server = "host.docker.internal:3306"  # docker用
    # server = "localhost:3306"
    db_name = "group2_db"
    df = pd.DataFrame({"id": [4, 5, 6], "name": ["a", "b", "c"]})
    with create_engine(
            f"mysql+pymysql://{username_sql}:{password_sql}@{server}/{db_name}",).connect() as conn:  # noqa
        df.to_sql(
            name="dockertest",
            con=conn,
            if_exists="append",
            index=False
        )
    print("OK")
    print("finish Task 2")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='test_docker_connection',
    default_args=default_args,
    description='An example DAG with Python operators',
    schedule="*/3 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False
)

# Define the tasks
task1_obj = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)

task2_obj = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

# Task dependencies
task1_obj >> task2_obj
