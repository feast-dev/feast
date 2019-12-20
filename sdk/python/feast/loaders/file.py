import shutil
import tempfile
from typing import Optional
from urllib.parse import urlparse
import uuid
import pandas as pd
from datetime import datetime
from google.cloud import storage
from pandavro import to_avro


def export_dataframe_to_staging_location(
    df: pd.DataFrame, staging_location_uri: str
) -> str:
    """
    Uploads a dataframe to a remote staging location

    Args:
        df: Pandas dataframe
        staging_location_uri: Remote staging location where dataframe should be written
            Examples:
                gs://bucket/path/
                file:///data/subfolder/

    Returns:
        Returns the full path to the file in the remote staging location
    """

    # Validate staging location
    uri = urlparse(staging_location_uri)
    if uri.scheme == "gs":
        dir_path, file_name, source_path = export_dataframe_to_local(df)
        upload_file_to_gcs(
            source_path, uri.hostname, str(uri.path).strip("/") + "/" + file_name
        )
        if len(str(dir_path)) < 5:
            raise Exception(f"Export location {dir_path} dangerous. Stopping.")
        shutil.rmtree(dir_path)
    elif uri.scheme == "file":
        dir_path, file_name, source_path = export_dataframe_to_local(df, uri.path)
    else:
        raise Exception(
            f"Staging location {staging_location_uri} does not have a valid URI. Only gs:// and file:// are supported"
        )

    return staging_location_uri.rstrip("/") + "/" + file_name


def export_dataframe_to_local(df: pd.DataFrame, dir_path: Optional[str] = None):
    """
    Exports a pandas dataframe to the local filesystem

    Args:
        df: Pandas dataframe to save
        dir_path: (optional) Absolute directory path '/data/project/subfolder/'
    """

    # Create local staging location if not provided
    if dir_path is None:
        dir_path = tempfile.mkdtemp()

    file_name = f'{datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p")}_{str(uuid.uuid4())[:8]}.avro'
    dest_path = f"{dir_path}/{file_name}"

    # Temporarily rename datetime column to event_timestamp. Ideally we would
    # force the schema with our avro writer instead.
    df.columns = ["event_timestamp" if col == "datetime" else col for col in df.columns]

    try:
        # Export dataset to file in local path
        to_avro(df=df, file_path_or_buffer=dest_path)
    except Exception:
        raise
    finally:
        # Revert event_timestamp column to datetime
        df.columns = [
            "datetime" if col == "event_timestamp" else col for col in df.columns
        ]

    return dir_path, file_name, dest_path


def upload_file_to_gcs(local_path: str, bucket: str, remote_path: str):
    """
    Upload a file from the local file system to Google Cloud Storage (GCS)

    Args:
        local_path: Local filesystem path of file to upload
        bucket: GCS bucket to upload to
        remote_path: Path within GCS bucket to upload file to, includes file name
    """

    storage_client = storage.Client(project=None)
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(local_path)
