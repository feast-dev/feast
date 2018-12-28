from feast.sdk.resources.feature_set import FeatureSet, DatasetInfo, FileType
from feast.sdk.utils.types import Granularity
from feast.sdk.utils.format import make_feature_id
from google.cloud.bigquery.table import Table

import time
import pytest
import os


class TestFeatureSet(object):
    def test_features(self):
        entity_name = "driver"
        features = ["driver.hour.feature1", "driver.hour.feature2"]

        feature_set = FeatureSet(entity_name, features)
        assert len(feature_set.features) == 2

        assert len(set(feature_set.features) & set(features)) == 2


class TestDatasetInfo(object):
    def test_creation(self):
        fake_table = "fake_table"
        with pytest.raises(TypeError,
                           match="table must be a BigQuery table type"):
            DatasetInfo(fake_table)

        table = Table.from_string("project_id.dataset_id.table_id")
        dataset = DatasetInfo(table)
        assert dataset._table == table

    def test_download_df(self, mocker):
        self._stop_time(mocker)
        mocked_gs_to_df = mocker.patch(
            "feast.sdk.resources.feature_set.gs_to_df",
            return_value=None)

        staging_path = "gs://temp/"
        staging_file_name = "temp_0"
        table = Table.from_string("project_id.dataset_id.table_id")
        dataset_info = DatasetInfo(table)

        exp_staging_path = os.path.join(staging_path, staging_file_name)
        mocker.patch.object(dataset_info._bq_client, "extract_table",
                            return_value=_Job())

        dataset_info.download_to_df(staging_path)

        assert len(dataset_info._bq_client.extract_table.call_args_list) == 1
        args, kwargs = dataset_info._bq_client.extract_table.call_args_list[0]
        assert args[0] == table
        assert args[1] == exp_staging_path
        assert kwargs['job_config'].destination_format == "CSV"
        mocked_gs_to_df.assert_called_once_with(exp_staging_path)

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
        table = Table.from_string("project_id.dataset_id.table_id")
        dataset = DatasetInfo(table)

        with pytest.raises(ValueError,
                           match="staging_uri must be a directory in "
                                 "GCS"):
            dataset.download("/tmp/dst", "/local/directory")

        with pytest.raises(ValueError,
                           match="staging_uri must be a directory in "
                                 "GCS"):
            dataset.download_to_df("/local/directory")

    def _test_download_file(self, mocker, type):
        staging_path = "gs://temp/"
        staging_file_name = "temp_0"
        dst_path = "/tmp/myfile.csv"
        table = Table.from_string("project_id.dataset_id.table_id")
        dataset_info = DatasetInfo(table)

        mock_blob = _Blob()
        mocker.patch.object(mock_blob, "download_to_filename")
        mocker.patch.object(dataset_info._bq_client, "extract_table",
                            return_value=_Job())
        mocker.patch.object(dataset_info._gcs_client, "get_bucket",
                            return_value=_Bucket(mock_blob))

        dataset_info.download(dst_path, staging_path, type=type)

        exp_staging_path = os.path.join(staging_path, staging_file_name)
        assert len(dataset_info._bq_client.extract_table.call_args_list) == 1
        args, kwargs = dataset_info._bq_client.extract_table.call_args_list[0]
        assert args[0] == table
        assert args[1] == exp_staging_path
        assert kwargs['job_config'].destination_format == str(type)

        mock_blob.download_to_filename.assert_called_once_with(dst_path)

    def _stop_time(self, mocker):
        mocker.patch('time.time')
        time.time.return_value = 0


class _Job():
    def result(self):
        return None


class _Bucket():
    def __init__(self, blob):
        self._blob = blob

    def blob(self, blob_name):
        return self._blob


class _Blob():
    def download_to_filename(self, filename):
        pass
