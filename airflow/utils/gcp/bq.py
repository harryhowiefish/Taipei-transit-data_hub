from typing import List

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound


def build_bq_from_gcs(
    client: bigquery.Client,
    dataset_name: str,
    table_name: str,
    bucket_name: str,
    blob_name: str,
    schema: List[bigquery.SchemaField] = None,
    filetype: str = "parquet",
) -> bool:
    """
    Build a bigquery external table from a file in GCS.

    Args:
        client (bigquery.Client): The client to use to create the external table.
        dataset_name (str): The name of the dataset to create.
        table_name (str): The name of the table to create.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        schema (List[bigquery.SchemaField], optional): 
        The schema of the table to upload to. Default is None.
        If None, use the default schema (automatic-detect).
        filetype (str): The type of the file to download. Default is "parquet". 
        Can be "parquet" or "csv" or "jsonl".

    Returns:
        bool: True if the upload was successful, False otherwise.
    """  # noqa
    # Construct the fully-qualified BigQuery table ID
    table_id = f"{client.project}.{dataset_name}.{table_name}"

    try:
        client.get_table(table_id)  # Attempt to get the table
        print(f"Table {table_id} already exists.")
        return False
    except NotFound:
        # Define the external data source configuration
        if filetype == "parquet":
            external_config = bigquery.ExternalConfig("PARQUET")
        elif filetype == "csv":
            external_config = bigquery.ExternalConfig("CSV")
        elif filetype == "jsonl":
            external_config = bigquery.ExternalConfig("JSONL")
        else:
            raise ValueError(
                f"Invalid filetype: {
                    filetype}. Please specify 'parquet' or 'csv' or 'jsonl'."
            )
        external_config.source_uris = [f"gs://{bucket_name}/{blob_name}"]
        if schema:
            external_config.schema = schema
        # Create a table with the external data source configuration
        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        try:
            # API request to create the external table
            client.create_table(table)
            print(f"External table {table.table_id} created.")
            return True
        except Exception as e:
            raise Exception(f"Failed to create external table, reason: {e}")
    except Exception as e:
        raise Exception(
            f"An error occurred while checking if the table exists: {e}")


def query_bq(client: bigquery.Client, sql_query: str) -> bigquery.QueryJob:
    """
    Query bigquery and return results. (可以用在bigquery指令，例如Insert、Update，但沒有要取得資料表的資料)

    Args:
        client (bigquery.Client): The client to use to query bigquery.
        sql_query (str): The SQL query to execute.

    Returns:
        bigquery.QueryJob: The result of the query.
    """
    try:
        query_job = client.query(sql_query)
        return query_job.result()  # Return the results for further processing
    except Exception as e:
        raise Exception(f"Failed to query bigquery table, reason: {e}")


def query_bq_to_df(client: bigquery.Client, sql_query: str) -> pd.DataFrame:
    """
    Executes a BigQuery SQL query and directly loads the results into a DataFrame
    using the BigQuery Storage API.  (可以用在bigquery指令，然後取得資料表的資料成為DataFrame)

    Args:
        client (bigquery.Client): The client to use to query bigquery.
        query (str): SQL query string.

    Returns:
        pd.DataFrame: The query results as a Pandas DataFrame.
    """
    try:
        query_job = client.query(sql_query)
        return query_job.to_dataframe()  # Convert result to DataFrame
    except Exception as e:
        raise Exception(f"Failed to query bigquery table, reason: {e}")


def upload_df_to_bq(
    client: bigquery.Client,
    df: pd.DataFrame,
    dataset_name: str,
    table_name: str,
    schema: List[bigquery.SchemaField] = None,
    filetype: str = "parquet",
) -> bool:
    """
    Upload a pandas dataframe to bigquery.

    Args:
        client (bigquery.Client): The client to use to upload to bigquery.
        df (pd.DataFrame): The dataframe to upload.
        dataset_name (str): The name of the dataset to upload to.
        table_name (str): The name of the table to upload to.
        schema (List[bigquery.SchemaField], optional): The schema of the table to upload to. Default is None.
                                                        If None, use the default schema (automatic-detect).
        filetype (str): The type of the file to download. Default is "parquet". Can be "parquet" or "csv" or "jsonl".

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    dataset_id = client.dataset(dataset_name)
    table_id = dataset_id.table(table_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if filetype == "parquet":
        job_config.source_format = bigquery.SourceFormat.PARQUET
    elif filetype == "csv":
        job_config.source_format = bigquery.SourceFormat.CSV
    elif filetype == "jsonl":
        job_config.source_format = bigquery.SourceFormat.JSONL
    else:
        raise ValueError(
            f"Invalid filetype: {
                filetype}. Please specify 'parquet' or 'csv' or 'jsonl'."
        )
    if schema:
        job_config.schema = schema

    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        table = client.get_table(table_id)
        print(f"Table {table.table_id} created with {table.num_rows} rows.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload df to bigquery, reason: {e}")


def delete_table(client: bigquery.Client, dataset_name: str, table_name: str) -> bool:
    """
    Delete a bigquery table.

    Args:
        client (bigquery.Client): The client to use to delete the table.
        dataset_name (str): The name of the dataset to delete the table from.
        table_name (str): The name of the table to delete.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    table_id = f"{dataset_name}.{table_name}"
    try:
        client.delete_table(table_id)
        print(f"Table {table_id} deleted.")
    except NotFound:
        print(f"Table {table_id} not found.")
        return False
    return True


def rename_table(
    client: bigquery.Client, dataset_name: str, table_name: str, new_table_name: str
) -> bool:
    """
    Rename a BigQuery table by creating a new table with the new name and copying data from the old table,
    then deleting the old table. Handles both regular and external tables.

    Args:
        client (bigquery.Client): The client to use with BigQuery.
        dataset_name (str): The name of the dataset containing the table.
        table_name (str): The current name of the table.
        new_table_name (str): The new name for the table.

    Returns:
        bool: True if the rename was successful, False otherwise.
    """
    dataset_ref = client.dataset(dataset_name)
    old_table_ref = dataset_ref.table(table_name)
    new_table_ref = dataset_ref.table(new_table_name)

    try:
        old_table = client.get_table(old_table_ref)

        if old_table.table_type == "EXTERNAL":
            # Create a new external table with the same configuration
            new_table = bigquery.Table(new_table_ref, schema=old_table.schema)
            new_table.external_data_configuration = (
                old_table.external_data_configuration
            )
            client.create_table(new_table)
            print(f"External table {table_name} renamed to {new_table_name}.")

            # Optionally delete the old external table
            client.delete_table(old_table_ref)
            print(f"Old external table {table_name} deleted.")
        else:
            # Handle regular tables
            new_table = bigquery.Table(new_table_ref, schema=old_table.schema)
            new_table.time_partitioning = old_table.time_partitioning
            new_table.range_partitioning = old_table.range_partitioning
            new_table.clustering_fields = old_table.clustering_fields
            new_table.description = old_table.description
            client.create_table(new_table)

            # Copy data from the old table to the new table
            job = client.copy_table(old_table_ref, new_table_ref)
            job.result()  # Wait for the job to complete

            # Delete the old table
            client.delete_table(old_table_ref)
            print(f"Table {table_name} renamed to {new_table_name}.")

        return True
    except NotFound:
        print(f"Table {table_name} not found.")
        return False
    except Conflict:
        print(f"Conflict occurred: Table {new_table_name} already exists.")
        return False
    except Exception as e:
        raise Exception(f"Failed to rename table, reason: {e}")
