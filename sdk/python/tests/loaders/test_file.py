#
# Copyright 2020 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import tempfile
from unittest.mock import patch
from urllib.parse import urlparse

import boto3
import fastavro
import pandas as pd
import pandavro
from moto import mock_s3
from pandas.testing import assert_frame_equal
from pytest import fixture

from feast.loaders.file import export_source_to_staging_location

BUCKET = "test_bucket"
FOLDER_NAME = "test_folder"
FILE_NAME = "test.avro"

LOCAL_FILE = "file:///tmp/tmp"
S3_LOCATION = f"s3://{BUCKET}/{FOLDER_NAME}"

TEST_DATA_FRAME = pd.DataFrame(
    {
        "driver": [1001, 1002, 1003],
        "transaction": [1001, 1002, 1003],
        "driver_id": [1001, 1002, 1003],
    }
)


@fixture
def avro_data_path():
    final_results = tempfile.mktemp()
    pandavro.to_avro(file_path_or_buffer=final_results, df=TEST_DATA_FRAME)
    return final_results


@patch("feast.loaders.file._get_file_name", return_value=FILE_NAME)
def test_export_source_to_staging_location_local_file_should_pass(get_file_name):
    source = export_source_to_staging_location(TEST_DATA_FRAME, LOCAL_FILE)
    assert source == [f"{LOCAL_FILE}/{FILE_NAME}"]
    assert get_file_name.call_count == 1


@mock_s3
@patch("feast.loaders.file._get_file_name", return_value=FILE_NAME)
def test_export_source_to_staging_location_dataframe_to_s3_should_pass(get_file_name):
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=BUCKET)
    source = export_source_to_staging_location(TEST_DATA_FRAME, S3_LOCATION)
    file_obj = tempfile.TemporaryFile()
    uri = urlparse(source[0])
    s3_client.download_fileobj(uri.hostname, uri.path[1:], file_obj)
    file_obj.seek(0)
    avro_reader = fastavro.reader(file_obj)
    retrived_df = pd.DataFrame.from_records([r for r in avro_reader])
    assert_frame_equal(retrived_df, TEST_DATA_FRAME, check_like=True)
    assert get_file_name.call_count == 1


def test_export_source_to_staging_location_s3_file_as_source_should_pass():
    source = export_source_to_staging_location(S3_LOCATION, None)
    assert source == [S3_LOCATION]


@mock_s3
def test_export_source_to_staging_location_s3_wildcard_as_source_should_pass(
    avro_data_path,
):
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=BUCKET)
    with open(avro_data_path, "rb") as data:
        s3_client.upload_fileobj(data, BUCKET, f"{FOLDER_NAME}/file1.avro")
    with open(avro_data_path, "rb") as data:
        s3_client.upload_fileobj(data, BUCKET, f"{FOLDER_NAME}/file2.avro")
    sources = export_source_to_staging_location(f"{S3_LOCATION}/*", None)
    assert sources == [f"{S3_LOCATION}/file1.avro", f"{S3_LOCATION}/file2.avro"]
