import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


def create_dataset(dataset_name: str, client: bigquery.Client, location: str = "asia-east1"):
    """Create a dataset"""
    porject_name = client.project
    dataset_id = f"{porject_name}.{dataset_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location

    client.create_dataset(dataset)
    print(
        f"created {dataset_name} in project {porject_name} ,location:{location}")


def delete_dataset(dataset_name: str, client: bigquery.Client):  # 刪除dataset名稱
    porject_name = client.project
    dataset_id = f"{porject_name}.{dataset_name}"
    client.delete_dataset(dataset=dataset_id,
                          delete_contents=True, not_found_ok=True)
    print(f"dataset {dataset_name} in project {porject_name} has been delete")


def delete_table(dataset_name, table_name, client: bigquery.Client):
    table_ref = client.dataset(dataset_name).table(table_name)
    client.delete_table(table_ref)
    print("Table '{}' successfully deleted.".format(table_name))


if __name__ == "__main__":
    BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
    # BIGQUERY_CREDENTIALS_FILE_PATH = r"C:\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
    BQ_CLIENT = bigquery.Client()
    # create_dataset(dataset_name="ANDY_ETL_SRC", client=BQ_CLIENT)
    # create_dataset(dataset_name="ANDY_ETL_ODS", client=BQ_CLIENT)
    # create_dataset(dataset_name="ANDY_ETL_FACT", client=BQ_CLIENT)
    # create_dataset(dataset_name="ANDY_ETL_DIM", client=BQ_CLIENT)
    create_dataset(dataset_name="ANDY_ETL_MART", client=BQ_CLIENT)
    # delete_dataset("create_test")
