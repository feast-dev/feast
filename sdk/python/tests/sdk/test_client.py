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

from datetime import datetime

import grpc
import numpy as np
import pandas as pd
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from pandas.util.testing import assert_frame_equal

import feast.core.CoreService_pb2_grpc as core
import feast.core.DatasetService_pb2_grpc as training
import feast.core.JobService_pb2_grpc as jobs
import feast.serving.Serving_pb2 as serving_pb
from feast.core.CoreService_pb2 import CoreServiceTypes
from feast.core.DatasetService_pb2 import DatasetInfo as DatasetInfo_pb
from feast.core.DatasetService_pb2 import DatasetServiceTypes
from feast.core.JobService_pb2 import JobServiceTypes
from feast.sdk.client import Client, _parse_date, _timestamp_from_datetime
from feast.sdk.importer import Importer
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.feature_group import FeatureGroup
from feast.sdk.resources.feature_set import FeatureSet, DatasetInfo, FileType
from feast.sdk.resources.storage import Storage
from feast.sdk.utils.bq_util import TableDownloader
from feast.serving.Serving_pb2 import QueryFeaturesRequest, \
    QueryFeaturesResponse, FeatureValue
from feast.specs.FeatureSpec_pb2 import FeatureSpec, DataStores, DataStore
from feast.specs.ImportSpec_pb2 import ImportSpec
from feast.specs.StorageSpec_pb2 import StorageSpec
from feast.types.Value_pb2 import Value


class TestClient(object):
    @pytest.fixture
    def client(self, mocker):
        cli = Client(core_url="some.uri", serving_url="some.serving.uri")
        mocker.patch.object(cli, '_connect_core')
        mocker.patch.object(cli, '_connect_serving')
        return cli

    def test_apply_single_feature(self, client, mocker):
        my_feature = Feature(name="test", entity="test")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(
                grpc_stub,
                'ApplyFeature',
                return_value=CoreServiceTypes.ApplyFeatureResponse(
                    featureId="test.test")):
            client._core_service_stub = grpc_stub
            id = client.apply(my_feature)
            assert id == "test.test"

    def test_apply_single_entity(self, client, mocker):
        my_entity = Entity(name="test")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(
                grpc_stub,
                'ApplyEntity',
                return_value=CoreServiceTypes.ApplyEntityResponse(
                    entityName="test")):
            client._core_service_stub = grpc_stub
            name = client.apply(my_entity)
            assert name == "test"

    def test_apply_single_feature_group(self, client, mocker):
        my_feature_group = FeatureGroup(id="test")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(
                grpc_stub,
                'ApplyFeatureGroup',
                return_value=CoreServiceTypes.ApplyFeatureGroupResponse(
                    featureGroupId="test")):
            client._core_service_stub = grpc_stub
            name = client.apply(my_feature_group)
            assert name == "test"

    def test_apply_single_storage(self, client, mocker):
        my_storage = Storage(id="TEST", type="redis")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(
                grpc_stub,
                'ApplyStorage',
                return_value=CoreServiceTypes.ApplyStorageResponse(
                    storageId="TEST")):
            client._core_service_stub = grpc_stub
            name = client.apply(my_storage)
            assert name == "TEST"

    def test_apply_unsupported_object(self, client):
        with pytest.raises(TypeError) as e_info:
            client.apply(None)
            assert e_info.__str__() == "Apply can only be passed one of the" \
                   + "following types: [Feature, Entity, FeatureGroup, Storage, Importer]"

    def test_apply_multiple(self, client, mocker):
        my_storage = Storage(id="TEST", type="redis")
        my_feature_group = FeatureGroup(id="test")
        my_entity = Entity(name="test")

        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        mocker.patch.object(
            grpc_stub,
            'ApplyStorage',
            return_value=CoreServiceTypes.ApplyStorageResponse(
                storageId="TEST"))
        mocker.patch.object(
            grpc_stub,
            'ApplyFeatureGroup',
            return_value=CoreServiceTypes.ApplyFeatureGroupResponse(
                featureGroupId="test"))
        mocker.patch.object(
            grpc_stub,
            'ApplyEntity',
            return_value=CoreServiceTypes.ApplyEntityResponse(
                entityName="test"))

        client._core_service_stub = grpc_stub
        ids = client.apply([my_storage, my_entity, my_feature_group])
        assert ids == ["TEST", "test", "test"]

    def test_run_job_no_staging(self, client, mocker):
        grpc_stub = jobs.JobServiceStub(grpc.insecure_channel(""))

        mocker.patch.object(
            grpc_stub,
            'SubmitJob',
            return_value=JobServiceTypes.SubmitImportJobResponse(
                jobId="myjob12312"))
        client._job_service_stub = grpc_stub
        importer = Importer({"import": ImportSpec()}, None,
                            {"require_staging": False})

        job_id = client.run(importer)
        assert job_id == "myjob12312"

    def test_create_dataset_invalid_args(self, client):
        feature_set = FeatureSet("entity", ["entity.feature1"])
        # empty feature set
        with pytest.raises(ValueError, match="feature set is empty"):
            inv_feature_set = FeatureSet("entity", [])
            client.create_dataset(inv_feature_set, "2018-12-01", "2018-12-02")
        # invalid start date
        with pytest.raises(
                ValueError,
                match="Incorrect date format, should be YYYY-MM-DD"):
            client.create_dataset(feature_set, "20181201", "2018-12-02")
        # invalid end date
        with pytest.raises(
                ValueError,
                match="Incorrect date format, should be YYYY-MM-DD"):
            client.create_dataset(feature_set, "2018-12-01", "20181202")
        # start date  > end date
        with pytest.raises(ValueError, match="end_date is before start_date"):
            client.create_dataset(feature_set, "2018-12-02", "2018-12-01")
        # invalid limit
        with pytest.raises(
                ValueError, match="limit is not a positive integer"):
            client.create_dataset(feature_set, "2018-12-01", "2018-12-02", -1)

    def test_create_dataset(self, client, mocker):
        entity_name = "myentity"
        feature_ids = ["myentity.feature1", "myentity.feature2"]
        fs = FeatureSet(entity_name, feature_ids)
        start_date = "2018-01-02"
        end_date = "2018-12-31"

        ds_pb = DatasetInfo_pb(
            name="dataset_name", tableUrl="project.dataset.table")

        mock_trn_stub = training.DatasetServiceStub(grpc.insecure_channel(""))
        mocker.patch.object(
            mock_trn_stub,
            "CreateDataset",
            return_value=DatasetServiceTypes.CreateDatasetResponse(
                datasetInfo=ds_pb))
        client._dataset_service_stub = mock_trn_stub

        ds = client.create_dataset(fs, start_date, end_date)

        assert "dataset_name" == ds.name
        assert "project.dataset.table" == ds.table_id
        mock_trn_stub.CreateDataset.assert_called_once_with(
            DatasetServiceTypes.CreateDatasetRequest(
                featureSet=fs.proto,
                startDate=_timestamp_from_datetime(_parse_date(start_date)),
                endDate=_timestamp_from_datetime(_parse_date(end_date)),
                limit=None,
                namePrefix=None))

    def test_create_dataset_with_limit(self, client, mocker):
        entity_name = "myentity"
        feature_ids = ["myentity.feature1", "myentity.feature2"]
        fs = FeatureSet(entity_name, feature_ids)
        start_date = "2018-01-02"
        end_date = "2018-12-31"
        limit = 100

        ds_pb = DatasetInfo_pb(
            name="dataset_name", tableUrl="project.dataset.table")

        mock_trn_stub = training.DatasetServiceStub(grpc.insecure_channel(""))
        mocker.patch.object(
            mock_trn_stub,
            "CreateDataset",
            return_value=DatasetServiceTypes.CreateDatasetResponse(
                datasetInfo=ds_pb))
        client._dataset_service_stub = mock_trn_stub

        ds = client.create_dataset(fs, start_date, end_date, limit=limit)

        assert "dataset_name" == ds.name
        assert "project.dataset.table" == ds.table_id
        mock_trn_stub.CreateDataset.assert_called_once_with(
            DatasetServiceTypes.CreateDatasetRequest(
                featureSet=fs.proto,
                startDate=_timestamp_from_datetime(_parse_date(start_date)),
                endDate=_timestamp_from_datetime(_parse_date(end_date)),
                limit=limit,
                namePrefix=None))

    def test_create_dataset_with_name_prefix(self, client, mocker):
        entity_name = "myentity"
        feature_ids = ["myentity.feature1", "myentity.feature2"]
        fs = FeatureSet(entity_name, feature_ids)
        start_date = "2018-01-02"
        end_date = "2018-12-31"
        limit = 100
        name_prefix = "feast"

        ds_pb = DatasetInfo_pb(
            name="dataset_name", tableUrl="project.dataset.table")

        mock_dssvc_stub = training.DatasetServiceStub(
            grpc.insecure_channel(""))
        mocker.patch.object(
            mock_dssvc_stub,
            "CreateDataset",
            return_value=DatasetServiceTypes.CreateDatasetResponse(
                datasetInfo=ds_pb))
        client._dataset_service_stub = mock_dssvc_stub

        ds = client.create_dataset(
            fs, start_date, end_date, limit=limit, name_prefix=name_prefix)

        assert "dataset_name" == ds.name
        assert "project.dataset.table" == ds.table_id
        mock_dssvc_stub.CreateDataset.assert_called_once_with(
            DatasetServiceTypes.CreateDatasetRequest(
                featureSet=fs.proto,
                startDate=_timestamp_from_datetime(_parse_date(start_date)),
                endDate=_timestamp_from_datetime(_parse_date(end_date)),
                limit=limit,
                namePrefix=name_prefix))

    def test_build_serving_request(self, client):
        feature_set = FeatureSet("entity", ["entity.feat1", "entity.feat2"])

        req = client._build_serving_request(feature_set, ["1", "2", "3"])
        expected = QueryFeaturesRequest(
            entityName="entity",
            entityId=["1", "2", "3"],
            featureId=feature_set.features)
        assert req == expected

    def test_serving_response_to_df(self, client):
        response = self._create_query_features_response(
            entity_name="entity",
            entities={
                "1": {
                    "entity.feat1": (1, Timestamp(seconds=1)),
                    "entity.feat2": (2, Timestamp(seconds=2))
                },
                "2": {
                    "entity.feat1": (3, Timestamp(seconds=3)),
                    "entity.feat2": (4, Timestamp(seconds=4))
                }
            })
        expected_df = pd.DataFrame({'entity': ["1", "2"],
                                    'entity.feat1': [1, 3],
                                    'entity.feat2': [2, 4]}) \
            .reset_index(drop=True)
        df = client._response_to_df(FeatureSet("entity", ["entity.feat1",
                                                          "entity.feat2"]),
                                    response) \
            .sort_values(['entity']) \
            .reset_index(drop=True)[expected_df.columns]
        assert_frame_equal(
            df,
            expected_df,
            check_dtype=False,
            check_column_type=False,
            check_index_type=False)

    def test_serving_response_to_df_with_missing_value(self, client):
        response = self._create_query_features_response(
            entity_name="entity",
            entities={
                "1": {
                    "entity.feat1": (1, Timestamp(seconds=1))
                },
                "2": {
                    "entity.feat1": (3, Timestamp(seconds=3)),
                    "entity.feat2": (4, Timestamp(seconds=4))
                }
            })
        expected_df = pd.DataFrame({'entity': ["1", "2"],
                                    'entity.feat1': [1, 3],
                                    'entity.feat2': [np.nan, 4]}) \
            .reset_index(drop=True)
        df = client._response_to_df(FeatureSet("entity", ["entity.feat1",
                                                          "entity.feat2"]),
                                    response) \
            .sort_values(['entity']) \
            .reset_index(drop=True)[expected_df.columns]
        assert_frame_equal(
            df,
            expected_df,
            check_dtype=False,
            check_column_type=False,
            check_index_type=False)

    def test_serving_response_to_df_with_missing_feature(self, client):
        response = self._create_query_features_response(
            entity_name="entity",
            entities={
                "1": {
                    "entity.feat1": (1, Timestamp(seconds=1))
                },
                "2": {
                    "entity.feat1": (3, Timestamp(seconds=3))
                }
            })
        expected_df = pd.DataFrame({'entity': ["1", "2"],
                                    'entity.feat1': [1, 3],
                                    'entity.feat2': [np.NaN, np.NaN]}) \
            .reset_index(drop=True)
        df = client._response_to_df(FeatureSet("entity", ["entity.feat1",
                                                          "entity.feat2"]),
                                    response) \
            .sort_values(['entity']) \
            .reset_index(drop=True)[expected_df.columns]
        assert_frame_equal(
            df,
            expected_df,
            check_dtype=False,
            check_column_type=False,
            check_index_type=False)

    def test_serving_response_to_df_no_data(self, client):
        response = QueryFeaturesResponse(entityName="entity")
        expected_df = pd.DataFrame(
            columns=['entity', 'entity.feat1', 'entity.feat2'])
        df = client._response_to_df(
            FeatureSet("entity", ["entity.feat1", "entity.feat2"]), response)
        assert_frame_equal(
            df,
            expected_df,
            check_dtype=False,
            check_column_type=False,
            check_index_type=False)

    def test_serving_response_to_df_with_time_filter(self, client):
        response = self._create_query_features_response(
            entity_name="entity",
            entities={
                "1": {
                    "entity.feat1": (1, Timestamp(seconds=1))
                },
                "2": {
                    "entity.feat1": (3, Timestamp(seconds=3))
                }
            })
        expected_df = pd.DataFrame({'entity': ["1", "2"],
                                    'entity.feat1': [np.NaN, 3],
                                    'entity.feat2': [np.NaN, np.NaN]}) \
            .reset_index(drop=True)
        start = datetime.utcfromtimestamp(2)
        end = datetime.utcfromtimestamp(5)
        df = client._response_to_df(FeatureSet("entity", ["entity.feat1",
                                                          "entity.feat2"]),
                                    response, start, end) \
            .sort_values(['entity']) \
            .reset_index(drop=True)[expected_df.columns]
        assert_frame_equal(
            df,
            expected_df,
            check_dtype=False,
            check_column_type=False,
            check_index_type=False)

    def test_serving_invalid_type(self, client):
        start = "2018-01-01T01:01:01"
        end = "2018-01-01T01:01:01"
        ts_range = [start, end]
        with pytest.raises(
                TypeError, match="start and end must be datetime "
                                 "type"):
            client.get_serving_data(
                FeatureSet("entity", ["entity.feat1", "entity.feat2"]),
                ["1234", "5678"], ts_range)

    def test_download_dataset_as_file(self, client, mocker):
        destination = "/tmp/dest_file"

        table_dlder = TableDownloader()
        mocker.patch.object(
            table_dlder, "download_table_as_file", return_value=destination)

        client._table_downloader = table_dlder
        table_id = "project.dataset.table"
        staging_location = "gs://gcs_bucket/"
        dataset = DatasetInfo("mydataset", table_id)

        result = client.download_dataset(
            dataset,
            destination,
            staging_location=staging_location,
            file_type=FileType.CSV)

        assert result == destination
        table_dlder.download_table_as_file.assert_called_once_with(
            table_id, destination, staging_location, FileType.CSV)

    def _create_query_features_response(self, entity_name, entities):
        response = QueryFeaturesResponse(entityName=entity_name)
        for entity_id, feature_map in entities.items():
            feature = {}
            for feature_id, feature_value in feature_map.items():
                feature[feature_id] = FeatureValue(
                    value=Value(int32Val=feature_value[0]),
                    timestamp=feature_value[1])
            entity_pb = serving_pb.Entity(features=feature)
            response.entities[entity_id].CopyFrom(entity_pb)

        return response

    def _create_feature_spec(self, feature_id, wh_id):
        wh_store = DataStore(id=wh_id)
        datastores = DataStores(warehouse=wh_store)
        return FeatureSpec(id=feature_id, dataStores=datastores)

    def _create_bq_spec(self, id, project, dataset):
        return StorageSpec(
            id=id,
            type="bigquery",
            options={
                "project": project,
                "dataset": dataset
            })
