import os
import re
import shutil
import tempfile
import uuid
from datetime import datetime
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
from google.cloud import storage
from pandavro import to_avro


def export_source_to_staging_location(
        source: Union[pd.DataFrame, str], staging_location_uri: str
) -> List[str]:
    """
    Uploads a DataFrame as an avro file to a remote staging location.

    Args:
        source (Union[pd.DataFrame, str]:
            Source of data to be staged. Can be a pandas DataFrame or a file
            path.

        staging_location_uri (str):
            Remote staging location where DataFrame should be written.
            Examples:
                * gs://bucket/path/
                * file:///data/subfolder/

    Returns:
        List[str]:
            Returns a list containing the full path to the file(s) in the
            remote staging location.
    """

    # Validate staging location
    uri = urlparse(staging_location_uri)
    if uri.scheme == "gs":
        if isinstance(source, pd.DataFrame):
            dir_path, file_name, source_path = export_dataframe_to_local(source)
        elif urlparse(source).scheme == "file":
            dir_path = None
            file_name = os.path.basename(source)
            source_path = source
        elif urlparse(source).scheme == "gs":
            input_source_url = urlparse(source)
            if "*" in source:
                return _get_files(
                    bucket=input_source_url.hostname,
                    path=input_source_url.path
                )
            else:
                return [source]
        else:
            raise Exception(f"Only DataFrame, gs:// string or file:// string "
                            f"are allowed")

        upload_file_to_gcs(
            source_path,
            uri.hostname,
            str(uri.path).strip("/") + "/" + file_name
        )

        # Handle for none type
        if len(str(dir_path)) < 5:
            raise Exception(
                f"Export location {dir_path} dangerous. Stopping.")

        shutil.rmtree(dir_path)
    else:
        raise Exception(
            f"Staging location {staging_location_uri} does not have a valid "
            f"URI. Only gs:// uri scheme is allowed."
        )

    return [staging_location_uri.rstrip("/") + "/" + file_name]


def export_dataframe_to_local(
        df: pd.DataFrame,
        dir_path: Optional[str] = None
) -> Tuple[str, str, str]:
    """
    Exports a pandas DataFrame to the local filesystem.

    Args:
        df (pd.DataFrame):
            Pandas DataFrame to save.

        dir_path (Optional[str]):
            Absolute directory path '/data/project/subfolder/'.

    Returns:
        Tuple[str, str, str]:
            Tuple of directory path, file name and destination path. The
            destination path can be obtained by concatenating the directory
            path and file name.
    """

    # Create local staging location if not provided
    if dir_path is None:
        dir_path = tempfile.mkdtemp()

    file_name = _get_file_name()
    dest_path = f"{dir_path}/{file_name}"

    # Temporarily rename datetime column to event_timestamp. Ideally we would
    # force the schema with our avro writer instead.
    df.columns = [
        "event_timestamp"
        if col == "datetime" else col
        for col in df.columns
    ]

    try:
        # Export dataset to file in local path
        to_avro(df=df, file_path_or_buffer=dest_path)
    except Exception:
        raise
    finally:
        # Revert event_timestamp column to datetime
        df.columns = [
            "datetime"
            if col == "event_timestamp" else col
            for col in df.columns
        ]

    return dir_path, file_name, dest_path


def upload_file_to_gcs(local_path: str, bucket: str, remote_path: str) -> None:
    """
    Upload a file from the local file system to Google Cloud Storage (GCS).

    Args:
        local_path (str):
            Local filesystem path of file to upload.

        bucket (str):
            GCS bucket destination to upload to.

        remote_path (str):
            Path within GCS bucket to upload file to, includes file name.
    
    Returns:
        None:
            None
    """

    storage_client = storage.Client(project=None)
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(local_path)


def _get_files(bucket: str, path: str) -> List[str]:
    """
    List all available files within a Google storage bucket that matches a wild
    card path.

    Args:
        bucket (str):
            Google Storage bucket to reference.

        path (str):
            Wild card path containing the "*" character.
            Example:
                * /staginglocation/*.avro

    Returns:
        List[str]:
            List of all available files
    """

    storage_client = storage.Client(project=None)
    bucket = storage_client.get_bucket(bucket)
    if "*" in path:
        regex = re.compile(path.replace("*", ".*?").strip("/"))
        blob_list = bucket.list_blobs(
            prefix=path.strip("/").split("*")[0],
            delimiter="/"
        )
        blob_list = [x.name for x in blob_list]
        return [file for file in blob_list if re.match(regex, file)]
    else:
        raise Exception(f"{path} is not a wildcard path")


def _get_file_name() -> str:
    """
    Create a random file name.

    Returns:
        str:
            Randomised file name.
    """

    return f'{datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p")}_{str(uuid.uuid4())[:8]}.avro'
