from io import BytesIO
# from typing import List

import pandas as pd
from google.cloud import storage
# from google.cloud.exceptions import NotFound
import logging
# WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
# (Ref: https://github.com/googleapis/python-storage/issues/74)
storage.blob._MAX_MULTIPART_SIZE = 1024 * 1024  # 1 MB
storage.blob._DEFAULT_CHUNKSIZE = 1024 * 1024  # 1 MB
# End of Workaround


def upload_df_to_gcs(
    client: storage.Client, bucket_name: str, blob_name: str, df: pd.DataFrame
) -> bool:
    """
    Upload a pandas dataframe to GCS.

    Args:
        client (storage.Client): The client to use to upload to GCS.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        df (pd.DataFrame): The dataframe to upload.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if blob.exists():
        print("File already exists in GCP.")
        return False
    try:
        blob.upload_from_string(
            df.to_parquet(index=False), content_type="application/octet-stream"
        )
        print("Upload successful.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload pd.DataFrame to GCS, reason: {e}")


def upload_file_to_gcs(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    source_filepath: str
) -> bool:
    """
    Upload a file to GCS.

    Args:
        client (storage.Client): The client to use to upload to GCS.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        source_filepath (str): The path to the file to upload.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if blob.exists():
        print("File already exists.")
        return False
    try:
        blob.upload_from_filename(source_filepath)
        print("Upload successful.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload file to GCS, reason: {e}")


def download_df_from_gcs(
    client: storage.Client, bucket_name: str, blob_name: str
) -> pd.DataFrame:
    """
    Download a pandas dataframe from GCS.

    Args:
        client (storage.Client): The client to use to download from GCS.
        bucket_name (str): The name of the bucket to download from.
        blob_name (str): The name of the blob to download from.

    Returns:
        pd.DataFrame: The dataframe downloaded from GCS.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(
            f"Can't find {blob_name} in bucket {bucket_name}")

    bytes_data = blob.download_as_bytes()
    return pd.read_parquet(BytesIO(bytes_data))


def list_bucket(client: storage.Client, bucket_name: str,
                folder_name: str = None, verbose: bool = False) -> list:

    bucket = client.bucket(bucket_name)
    if folder_name:
        file_list = [file.name for file in bucket.list_blobs()
                     if folder_name in file.name]
    else:
        file_list = [file.name for file in bucket.list_blobs()]
    if verbose:
        print(file_list)
        return
    return file_list


def rename_blob(client: storage.Client, bucket_name: str,
                blob_name: str, new_blob_name: str):
    '''this will require GCS admin privilege'''
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    destination_bucket = bucket

    blob_copy = bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name
    )
    bucket.delete_blob(blob_name)

    logging.info(
        "Blob {} renamed as {}".format(
            source_blob.name,
            blob_copy.name,
        )
    )
