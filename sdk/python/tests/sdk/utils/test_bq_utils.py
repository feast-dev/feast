# Copyright 2018 The Feast Authors
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

import fastavro
import pandas as pd
import pytest
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound

from feast.sdk.resources.feature_set import FileType
from feast.sdk.utils.bq_util import TableDownloader, get_table_name, query_to_dataframe
from feast.specs.StorageSpec_pb2 import StorageSpec

testdata_path = os.path.abspath(os.path.join(__file__, "..", "..", "..", "data"))


def test_get_table_name():
    project_name = "my_project"
    dataset_name = "my_dataset"
    feature_id = "myentity.feature1"
    storage_spec = StorageSpec(
        id="BIGQUERY1",
        type="bigquery",
        options={"project": project_name, "dataset": dataset_name},
    )
    assert (
        get_table_name(feature_id, storage_spec)
        == "my_project.my_dataset.myentity"
    )


def test_get_table_name_not_bq():
    feature_id = "myentity.feature1"
    storage_spec = StorageSpec(id="REDIS1", type="redis")
    with pytest.raises(ValueError, match="storage spec is not BigQuery storage spec"):
        get_table_name(feature_id, storage_spec)


@pytest.mark.skipif(
    os.getenv("SKIP_BIGQUERY_TEST") is not None,
    reason="SKIP_BIGQUERY_TEST is set in the environment",
)
def test_query_to_dataframe():
    with open(
        os.path.join(testdata_path, "austin_bikeshare.bikeshare_stations.avro"), "rb"
    ) as expected_file:
        avro_reader = fastavro.reader(expected_file)
        expected = pd.DataFrame.from_records(avro_reader)

    query = "SELECT * FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`"
    actual = query_to_dataframe(query)
    assert expected.equals(actual)


@pytest.mark.skipif(
    os.getenv("SKIP_BIGQUERY_TEST") is not None,
    reason="SKIP_BIGQUERY_TEST is set in the environment",
)
def test_query_to_dataframe_for_non_existing_dataset():
    query = "SELECT * FROM `bigquery-public-data.this_dataset_should_not_exists.bikeshare_stations`"
    with pytest.raises(NotFound):
        query_to_dataframe(query)


class TestTableDownloader(object):
    def test_download_table_as_df(self, mocker):
        self._stop_time(mocker)
        mocked_gcs_to_df = mocker.patch(
            "feast.sdk.utils.bq_util.gcs_to_df", return_value=None
        )

        staging_path = "gs://temp/"
        staging_file_name = "temp_0"
        full_table_id = "project_id.dataset_id.table_id"

        table_dldr = TableDownloader()
        exp_staging_path = os.path.join(staging_path, staging_file_name)

        table_dldr._bqclient = _Mock_BQ_Client()
        mocker.patch.object(table_dldr._bqclient, "extract_table", return_value=_Job())

        table_dldr.download_table_as_df(full_table_id, staging_location=staging_path)

        assert len(table_dldr._bqclient.extract_table.call_args_list) == 1
        args, kwargs = table_dldr._bqclient.extract_table.call_args_list[0]
        assert args[0].full_table_id == Table.from_string(full_table_id).full_table_id
        assert args[1] == exp_staging_path
        assert kwargs["job_config"].destination_format == "CSV"
        mocked_gcs_to_df.assert_called_once_with(exp_staging_path)

    def test_download_csv(self, mocker):
        self._stop_time(mocker)
        self._test_download_file(mocker, FileType.CSV)

    def test_download_avro(self, mocker):
        self._stop_time(mocker)
        self._test_download_file(mocker, FileType.AVRO)

    def test_download_json(self, mocker):
        self._stop_time(mocker)
        self._test_download_file(mocker, FileType.JSON)

    def test_download_invalid_staging_url(self):
        full_table_id = "project_id.dataset_id.table_id"
        table_dldr = TableDownloader()
        with pytest.raises(
            ValueError, match="staging_uri must be a directory in " "GCS"
        ):
            table_dldr.download_table_as_file(
                full_table_id, "/tmp/dst", "/local/directory", FileType.CSV
            )

        with pytest.raises(
            ValueError, match="staging_uri must be a directory in " "GCS"
        ):
            table_dldr.download_table_as_df(full_table_id, "/local/directory")

    def _test_download_file(self, mocker, type):
        staging_path = "gs://temp/"
        staging_file_name = "temp_0"
        dst_path = "/tmp/myfile.csv"
        full_table_id = "project_id.dataset_id.table_id"

        table_dldr = TableDownloader()
        mock_blob = _Blob()
        mocker.patch.object(mock_blob, "download_to_filename")
        table_dldr._bqclient = _Mock_BQ_Client()
        mocker.patch.object(table_dldr._bqclient, "extract_table", return_value=_Job())
        table_dldr._storageclient = _Mock_GCS_Client()
        mocker.patch.object(
            table_dldr._storageclient, "get_bucket", return_value=_Bucket(mock_blob)
        )

        table_dldr.download_table_as_file(
            full_table_id, dst_path, staging_location=staging_path, file_type=type
        )

        exp_staging_path = os.path.join(staging_path, staging_file_name)
        assert len(table_dldr._bqclient.extract_table.call_args_list) == 1
        args, kwargs = table_dldr._bqclient.extract_table.call_args_list[0]
        assert args[0].full_table_id == Table.from_string(full_table_id).full_table_id
        assert args[1] == exp_staging_path
        assert kwargs["job_config"].destination_format == str(type)

        mock_blob.download_to_filename.assert_called_once_with(dst_path)

    def _stop_time(self, mocker):
        mocker.patch("time.time", return_value=0)


class _Mock_BQ_Client:
    def extract_table(self):
        pass


class _Mock_GCS_Client:
    def get_bucket(self):
        pass


class _Job:
    def result(self):
        return None


class _Bucket:
    def __init__(self, blob):
        self._blob = blob

    def blob(self, name):
        return self._blob


class _Blob:
    def download_to_filename(self, filename):
        pass
