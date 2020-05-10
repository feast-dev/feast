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
import shutil
import tempfile
import uuid
from datetime import datetime
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
from pandavro import to_avro

from feast.serving.ServingService_pb2 import DataFormat
from feast.staging.storage_client import get_staging_client


def export_source_to_staging_location(
    source: Union[pd.DataFrame, str], staging_location_uri: str, data_format: DataFormat  # type: ignore
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
                * S3 Avro file


        staging_location_uri (str):
            Remote staging location where DataFrame should be written.
            Examples:
                * gs://bucket/path/
                * s3://bucket/path/
                * file:///data/subfolder/
         data_format (DataFormat): Data format of files that are persisted in staging location during retrieval.

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
            df=source, dir_path=uri_path, data_format=data_format
        )
    elif isinstance(source, str):
        source_uri = urlparse(source)
        if source_uri.scheme in ["", "file"]:
            # Local file provided as a source
            dir_path = ""
            file_name = os.path.basename(source)
            source_path = os.path.abspath(
                os.path.join(source_uri.netloc, source_uri.path)
            )
        else:
            # gs, s3 file provided as a source.
            return get_staging_client(source_uri.scheme).list_files(
                bucket=source_uri.hostname, path=source_uri.path
            )
    else:
        raise Exception(
            f"Only string and DataFrame types are allowed as a "
            f"source, {type(source)} was provided."
        )

    # Push data to required staging location
    get_staging_client(uri.scheme).upload_file(
        source_path, uri.hostname, str(uri.path).strip("/") + "/" + file_name,
    )

    # Clean up, remove local staging file
    if dir_path and isinstance(source, pd.DataFrame) and len(dir_path) > 4:
        shutil.rmtree(dir_path)

    return [staging_location_uri.rstrip("/") + "/" + file_name]


def export_dataframe_to_local(
    df: pd.DataFrame,
    dir_path: Optional[str] = None,
    data_format: DataFormat = DataFormat.DATA_FORMAT_AVRO,  # type: ignore
) -> Tuple[str, str, str]:
    """
    Exports a pandas DataFrame to the local filesystem.

    Args:
        df (pd.DataFrame):
            Pandas DataFrame to save.

        dir_path (Optional[str]):
            Absolute directory path '/data/project/subfolder/'.

        data_format: Format of file used during loading or retrieval

    Returns:
        Tuple[str, str, str]:
            Tuple of directory path, file name and destination path. The
            destination path can be obtained by concatenating the directory
            path and file name.
    """

    # Create local staging location if not provided
    if dir_path is None:
        dir_path = tempfile.mkdtemp()

    file_name = _get_file_name(data_format)
    dest_path = f"{dir_path}/{file_name}"

    # Temporarily rename datetime column to event_timestamp. Ideally we would
    # force the schema with our avro writer instead.
    df.columns = ["event_timestamp" if col == "datetime" else col for col in df.columns]

    try:

        # Export dataset to file in local path
        if data_format == DataFormat.DATA_FORMAT_AVRO:
            to_avro(df=df, file_path_or_buffer=dest_path)
        elif data_format == DataFormat.DATA_FORMAT_CSV:
            # TODO: Remove this hidden coupling to PostgreSQL "COPY"
            # The following code orders columns alphabetically (with event_timestamp last). This allows the COPY
            # method of PostgreSQL to map the CSV columns correctly when loading it
            columns = sorted(df.columns)
            columns.pop(columns.index("event_timestamp"))
            df[columns + ["event_timestamp"]].to_csv(
                dest_path,
                sep=",",
                header=True,
                index=False,
                date_format="%Y-%m-%d %H:%M:%S",
            )
        else:
            raise ValueError(f"Incorrect DataFormat provided: {data_format}")
    except Exception:
        raise
    finally:
        # Revert event_timestamp column to datetime
        df.columns = [
            "datetime" if col == "event_timestamp" else col for col in df.columns
        ]

    return dir_path, file_name, dest_path


def _get_file_name(data_format: DataFormat = DataFormat.DATA_FORMAT_AVRO) -> str:  # type: ignore
    """
    Create a random file name.

    Returns:
        str:
            Randomised file name.
            :param data_format: Format used to persist files during retrieval
    """
    if data_format == DataFormat.DATA_FORMAT_AVRO:
        extension = ".avro"
    elif data_format == DataFormat.DATA_FORMAT_CSV:
        extension = ".csv"
    else:
        raise ValueError(f"Could not determine DataFormat: {data_format}")

    return f'{datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%p")}_{str(uuid.uuid4())[:8]}{extension}'
