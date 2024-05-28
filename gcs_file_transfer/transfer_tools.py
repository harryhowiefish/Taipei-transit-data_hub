from io import BytesIO
import os
from google.cloud import storage
from google.cloud.exceptions import NotFound

# WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
# (Ref: https://github.com/googleapis/python-storage/issues/74)
storage.blob._MAX_MULTIPART_SIZE = 1024 * 1024  # 1 MB
storage.blob._DEFAULT_CHUNKSIZE = 1024 * 1024  # 1 MB
# End of Workaround


class GCS_to_GCS():
    """
    A class to handle copying and duplicating Google Cloud Storage (GCS) buckets and their contents.

    Init Parameters
    ----------
    source_client : storage.Client
        The GCS client for the source project.
    dest_client : storage.Client
        The GCS client for the destination project.

    """

    def __init__(self,
                 source_client: storage.Client,
                 dest_client: storage.Client) -> None:
        self.source_client = source_client
        self.dest_client = dest_client

    def init_dest_bucket_setting(self,
                                 suffix: str,
                                 location: str = 'asia-east1'):
        """
        Initialize the destination bucket settings.

        Parameters
        ----------
        suffix : str
            The suffix to be added to new bucket names.
        location : str, optional
            The location for the new buckets, by default 'asia-east1'.
        """
        self.suffix = suffix
        self.gcs_location = location
        print(f'setting updated, new buckets will have the suffix: {suffix}')

    def duplicate_buckets_in_dest(self,
                                  bucket_names: list[str] = None) -> None:
        """
        Duplicate buckets from the source to the destination project.

        Parameters
        ----------
        bucket_names : list of str, optional
            List of bucket names to duplicate. If None, all buckets in the source project will be copied.

        Return
        --------
        None

        Raises
        ------
        ReferenceError
            If `init_dest_bucket_setting` has not been run.
        NotFound
            If a specified bucket name does not exist in the source project.
        """
        if self.suffix is None or self.gcs_location is None:
            ReferenceError('please run init_dest_bucket_setting first!')

        create_count = 0
        if bucket_names is None:
            bucket_names = [
                bucket.name for bucket in self.source_client.list_buckets()]
        else:
            for name in bucket_names:
                if self.source_client.bucket(name).exists() is False:
                    raise NotFound(f'could not find the {name} in source project')  # noqa
        for name in bucket_names:
            name_with_suffix = f'{name}_{self.suffix}'
            if self.dest_client.bucket(name_with_suffix).exists():
                print(f'{name_with_suffix} already exist!')
                continue
            self.dest_client.create_bucket(
                bucket_or_name=name_with_suffix, location=self.location)
            create_count += 1
        print(f"{create_count} buckets created in destination")

    def copy_all_files_from_single_bucket(
            self,
            bucket_name: str,
            verbose: int = 0) -> None:
        """
        Copy all files from a single bucket in the source project
        to the corresponding bucket in the destination project.

        Parameters
        ----------
        bucket_name : str
            The name of the source bucket.
        verbose : int, optional
            The verbosity level, by default 0.
            Set verbose to 1 for detail.

        Return
        --------
        None
        """
        src_bucket = self.source_client.bucket(bucket_name)
        dest_bucket = self.dest_client.bucket(f"{bucket_name}_{self.suffix}")
        file_count = sum(1 for _ in src_bucket.list_blobs())
        blob_list: list[storage.Blob] = src_bucket.list_blobs()
        for idx, blob in enumerate(blob_list):
            if idx % 10 == 1:
                print(f'process: {idx}/{file_count}')
            if dest_bucket.blob(blob.name).exists():
                if verbose > 0:
                    print(f'the file {blob.name} already exist in destination!')  # noqa
                continue
            data = blob.download_as_bytes()
            new_blob = dest_bucket.blob(blob.name)
            new_blob.upload_from_file(BytesIO(data))
            if verbose > 0:
                print(f'{blob.name} copied successfully')

    def copy_all_files(self):
        """
        Copy all files from all buckets in the source project to the corresponding buckets in the destination project.
        """
        bucket_names = [
            bucket.name for bucket in self.source_client.list_buckets()]
        bucket_count = len(bucket_names)
        for idx, name in enumerate(bucket_names):
            print('=======================================')
            print(f'moving files from {name}, {idx}/{bucket_count} buckets...')
            self.copy_all_files_from_single_bucket(name)


def download_bucket(client: storage.Client,
                    bucket_name: str,
                    local_dir: str = None,
                    verbose: int = 0) -> None:
    """
    Download all the blobs in the bucket to the local directory.

    Parameters
    ----------
    client : storage.Client
        The GCS client.
    bucket_name : str
        The name of the bucket to download.
    local_dir : str, optional
        The local directory to download the files to, by default None.
        If None, a directory with the bucket's name will be created.
    verbose : int, optional
        The verbosity level, by default 0.
        Set verbose to 1 for detail.
    """
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    if local_dir is None:
        local_dir = f'./{bucket_name}'
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    for blob in blobs:
        # Define the local path
        local_path = os.path.join(local_dir, blob.name)
        # Create the local directory structure if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        # Download the blob to the local file
        blob.download_to_filename(local_path)
        if verbose > 0:
            print(f"Downloaded {blob.name} to {local_path}")
    print(f'bucket {bucket_name} download complete')
    print(f'A total of {blobs.num_results} was downloaded')
