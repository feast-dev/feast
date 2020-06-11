import tempfile
from unittest.mock import patch, Mock
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

LOCAL_FILE = "file://tmp/tmp"
S3_LOCATION = f"s3://{BUCKET}/{FOLDER_NAME}"

ACCOUNT = "testaccount.blob.core.windows.net"
CONTAINER = "test_container"
WASB_LOCATION = f"wasbs://{ACCOUNT}/{CONTAINER}/{FOLDER_NAME}"

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


@patch("azure.common.credentials.get_azure_cli_credentials")
@patch("azure.storage.blob.ContainerClient")
@patch("feast.loaders.file._get_file_name", return_value=FILE_NAME)
def test_export_source_to_staging_location_dataframe_to_wasb_should_pass(
    get_file_name, client, get_credentials
):
    # Arrange
    client2 = Mock()
    retrived_df = None

    def capture_upload_blob(*args, **kwargs):
        assert kwargs["name"] == f"{FOLDER_NAME}/test.avro"
        nonlocal retrived_df
        avro_reader = fastavro.reader(kwargs["data"])
        retrived_df = pd.DataFrame.from_records([r for r in avro_reader])

    client2.upload_blob = capture_upload_blob

    client.return_value = client2
    credential = Mock()
    get_credentials.return_value = credential, Mock()

    # Act
    source = export_source_to_staging_location(TEST_DATA_FRAME, WASB_LOCATION)

    # Assert
    assert source == [f"{WASB_LOCATION}/test.avro"]
    assert_frame_equal(retrived_df, TEST_DATA_FRAME, check_like=True)
    assert get_file_name.call_count == 1


def test_export_source_to_staging_location_wasb_file_as_source_should_pass():
    source = export_source_to_staging_location(WASB_LOCATION, None)
    assert source == [WASB_LOCATION]


@patch("azure.common.credentials.get_azure_cli_credentials")
@patch("azure.storage.blob.ContainerClient")
def test_export_source_to_staging_location_wasb_wildcard_as_source_should_pass(
    client, get_credentials, avro_data_path,
):
    # Arrange
    class blob1:
        name = f"{FOLDER_NAME}/file1.avro"

    class blob2:
        name = f"{FOLDER_NAME}/file2.avro"

    client2 = Mock()
    client2.list_blobs.return_value = [blob1, blob2]
    client.from_container_url.return_value = client2
    credential = Mock()
    get_credentials.return_value = credential, Mock()

    # Act
    sources = export_source_to_staging_location(f"{WASB_LOCATION}/*", None)

    # Assert
    client.from_container_url.assert_called_with(
        f"https://{ACCOUNT}/{CONTAINER}", credential=credential
    )
    client2.list_blobs.assert_called_with(name_starts_with=f"{FOLDER_NAME}/")
    assert sources == [f"{WASB_LOCATION}/file1.avro", f"{WASB_LOCATION}/file2.avro"]
