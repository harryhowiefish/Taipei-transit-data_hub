from io import BytesIO
from typing import List

import pandas as pd
from google.cloud import storage

# WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
# (Ref: https://github.com/googleapis/python-storage/issues/74)
storage.blob._MAX_MULTIPART_SIZE = 1024 * 1024  # 1 MB
storage.blob._DEFAULT_CHUNKSIZE = 1024 * 1024  # 1 MB
# End of Workaround


def upload_df_to_gcs(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    df: pd.DataFrame,
    filetype: str = "parquet",
) -> bool:
    """
    Upload a pandas dataframe to GCS.

    Args:
        client (storage.Client): The client to use to upload to GCS.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        df (pd.DataFrame): The dataframe to upload.
        filetype (str): The type of the file to download. Default is "parquet".
                        Can be "parquet" or "csv" or "jsonl".

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if blob.exists():
        print("File already exists in GCP.")
        return False
    try:
        if filetype == "parquet":
            blob.upload_from_string(
                df.to_parquet(index=False), content_type="application/octet-stream"
            )
        elif filetype == "csv":
            blob.upload_from_string(
                df.to_csv(index=False), content_type="text/csv")
        elif filetype == "jsonl":
            blob.upload_from_string(
                df.to_json(orient="records", lines=True),
                content_type="application/jsonl",
            )
        print("Upload successful.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload pd.DataFrame to GCS, reason: {e}")


def upload_file_to_gcs(
    client: storage.Client, bucket_name: str, blob_name: str, source_filepath: str
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
    client: storage.Client, bucket_name: str, blob_name: str, filetype: str = "parquet"
) -> pd.DataFrame:
    """
    Download a pandas dataframe from GCS.

    Args:
        client (storage.Client): The client to use to download from GCS.
        bucket_name (str): The name of the bucket to download from.
        blob_name (str): The name of the blob to download from.
        filetype (str): The type of the file to download. Default is "parquet".
                        Can be "parquet" or "csv" or "jsonl".

    Returns:
        pd.DataFrame: The dataframe downloaded from GCS.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(
            f"Can't find {blob_name}  bucket {bucket_name}")

    bytes_data = blob.download_as_bytes()
    data_io = BytesIO(bytes_data)

    if filetype == "csv":
        return pd.read_csv(data_io)
    elif filetype == "parquet":
        return pd.read_parquet(data_io)
    elif filetype == "jsonl":
        return pd.read_json(data_io, lines=True)
    else:
        raise ValueError(
            f"Invalid filetype: {
                filetype}. Please specify 'parquet' or 'csv' or 'jsonl'."
        )


def delete_blob(client, bucket_name, blob_name) -> bool:
    """
    Delete a blob from GCS.

    Args:
        client (storage.Client): The client to use to interact with Google Cloud Storage.
        bucket_name (str): The name of the bucket.
        blob_name (str): The name of the blob to delete.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            print(f"Can't find {blob_name} in bucket {bucket_name}.")
            return False
        blob.delete()
        print(f"Blob {blob_name} deleted from bucket {bucket_name}.")
        return True
    except Exception as e:
        raise Exception(f"Failed to delete blob, reason: {e}")


def rename_blob(
    client: storage.Client, bucket_name: str, blob_name: str, new_blob_name: str
) -> bool:
    """
    Rename a blob in GCS by copying it to a new name and then deleting the original.

    Args:
        client (storage.Client): The client to use with GCS.
        bucket_name (str): The name of the bucket where the blob is stored.
        blob_name (str): The current name of the blob.
        new_blob_name (str): The new name for the blob.

    Returns:
        bool: True if the rename was successful, False otherwise.
    """
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    new_blob = bucket.blob(new_blob_name)

    if not blob.exists():
        print(f"Can't find {blob_name} in bucket {bucket_name}.")
        return False

    if new_blob.exists():
        print(f"Blob {new_blob_name} already exists in bucket {bucket_name}.")
        return False

    # Copy the blob to the new location
    bucket.copy_blob(blob, bucket, new_blob_name)

    # Delete the original blob
    blob.delete()

    print(f"Blob {blob_name} renamed to {
          new_blob_name} in bucket {bucket_name}.")
    return True


def list_blobs(
    client: storage.Client, bucket_name: str, blob_prefix: str = None
) -> List[storage.Blob]:
    """
    List all blobs in a specified GCS bucket optionally filtered by a prefix.

    Args:
        client (storage.Client): The client to use to interact with Google Cloud Storage.
        bucket_name (str): The name of the bucket.
        blob_prefix (str, optional): The prefix to filter blobs. Defaults to None.

    Returns:
        List[storage.Blob]: A list of blobs sorted by their names.
    """
    try:
        bucket = client.get_bucket(bucket_name)
        if blob_prefix:
            blobs = bucket.list_blobs(prefix=blob_prefix)
        else:
            blobs = bucket.list_blobs()

        return sorted(blobs, key=lambda x: x.name)
    except Exception as e:
        raise Exception(f"Failed to list blobs in bucket {
                        bucket_name}, reason: {e}")
