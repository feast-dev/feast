# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import shutil
import tempfile
import uuid
from datetime import datetime
from typing import List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

import pandas as pd
from google.cloud import storage
from pandavro import to_avro


def export_source_to_staging_location(
    source: Union[pd.DataFrame, str], staging_location_uri: str
) -> List[str]:
    """
    Uploads a DataFrame as an Avro file to a remote staging location.

    The local staging location specified in this function is used for E2E
    tests, please do not use it.

    Args:
        source (Union[pd.DataFrame, str]:
            Source of data to be staged. Can be a pandas DataFrame or a file
            path.

            Only three types of source are allowed:
                * Pandas DataFrame
                * Local Avro file
                * GCS Avro file


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

    uri = urlparse(staging_location_uri)

    # Prepare Avro file to be exported to staging location
    if isinstance(source, pd.DataFrame):
        # DataFrame provided as a source
        uri_path = None  # type: Optional[str]
        if uri.scheme == "file":
            uri_path = uri.path

        # Remote gs staging location provided by serving
        dir_path, file_name, source_path = export_dataframe_to_local(
            df=source, dir_path=uri_path
        )
    elif urlparse(source).scheme in ["", "file"]:
        # Local file provided as a source
        dir_path = None
        file_name = os.path.basename(source)
        source_path = os.path.abspath(
            os.path.join(urlparse(source).netloc, urlparse(source).path)
        )
    elif urlparse(source).scheme == "gs":
        # Google Cloud Storage path provided
        input_source_uri = urlparse(source)
        if "*" in source:
            # Wildcard path
            return _get_files(bucket=input_source_uri.hostname, uri=input_source_uri)
        else:
            return [source]
    else:
        raise Exception(
            f"Only string and DataFrame types are allowed as a "
            f"source, {type(source)} was provided."
        )

    # Push data to required staging location
    if uri.scheme == "gs":
        # Staging location is a Google Cloud Storage path
        upload_file_to_gcs(
            source_path, uri.hostname, str(uri.path).strip("/") + "/" + file_name
        )
    elif uri.scheme == "file":
        # Staging location is a file path
        # Used for end-to-end test
        pass
    else:
        raise Exception(
            f"Staging location {staging_location_uri} does not have a "
            f"valid URI. Only gs:// and file:// uri scheme are supported."
        )

    # Clean up, remove local staging file
    if dir_path and isinstance(source, pd.DataFrame) and len(str(dir_path)) > 4:
        shutil.rmtree(dir_path)

    return [staging_location_uri.rstrip("/") + "/" + file_name]


def export_dataframe_to_local(
    df: pd.DataFrame, dir_path: Optional[str] = None
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


def _get_files(bucket: str, uri: ParseResult) -> List[str]:
    """
    List all available files within a Google storage bucket that matches a wild
    card path.

    Args:
        bucket (str):
            Google Storage bucket to reference.

        uri (urllib.parse.ParseResult):
            Wild card uri path containing the "*" character.
            Example:
                * gs://feast/staging_location/*
                * gs://feast/staging_location/file_*.avro

    Returns:
        List[str]:
            List of all available files matching the wildcard path.
    """

    storage_client = storage.Client(project=None)
    bucket = storage_client.get_bucket(bucket)
    path = uri.path

    if "*" in path:
        regex = re.compile(path.replace("*", ".*?").strip("/"))
        blob_list = bucket.list_blobs(
            prefix=path.strip("/").split("*")[0], delimiter="/"
        )
        # File path should not be in path (file path must be longer than path)
        return [
            f"{uri.scheme}://{uri.hostname}/{file}"
            for file in [x.name for x in blob_list]
            if re.match(regex, file) and file not in path
        ]
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
