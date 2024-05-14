import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import db_dtypes

BIGQUERY_CREDENTIALS_FILE_PATH = r"D:\data_engineer\TIR_group2\TIR101_Group2\secrets\harry_GCS_BigQuery_write_cred.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BIGQUERY_CREDENTIALS_FILE_PATH
BQ_CLIENT = bigquery.Client()


def create_dataset(dataset_name: str, location: str = "asia-east1", client: bigquery.Client = BQ_CLIENT):
    """Create a dataset"""
    porject_name = client.project
    dataset_id = f"{porject_name}.{dataset_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location

    client.create_dataset(dataset)
    print(
        f"created {dataset_name} in project {porject_name} ,location:{location}")


def delete_dataset(dataset_name: str, client: bigquery.Client = BQ_CLIENT):  # 刪除dataset名稱
    porject_name = client.project
    dataset_id = f"{porject_name}.{dataset_name}"
    client.delete_dataset(dataset=dataset_id,
                          delete_contents=True, not_found_ok=True)
    print(f"dataset {dataset_name} in project {porject_name} has been delete")


def delete_table(dataset_name, table_name, client: bigquery.Client = BQ_CLIENT):
    table_ref = client.dataset(dataset_name).table(table_name)
    client.delete_table(table_ref)
    print("Table '{}' successfully deleted.".format(table_name))


if __name__ == "__main__":
    create_dataset("create_test")
    delete_dataset("create_test")
