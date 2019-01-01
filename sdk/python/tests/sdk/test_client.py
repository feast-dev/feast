import pytest

import grpc
from datetime import datetime
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp
from google.cloud.bigquery.table import Table

import feast.core.CoreService_pb2_grpc as core
import feast.core.JobService_pb2_grpc as jobs
from feast.specs.StorageSpec_pb2 import StorageSpec
from feast.specs.FeatureSpec_pb2 import FeatureSpec, DataStores, DataStore
from feast.specs.ImportSpec_pb2 import ImportSpec
from feast.core.CoreService_pb2 import CoreServiceTypes
from feast.core.JobService_pb2 import JobServiceTypes
from feast.serving.Serving_pb2 import QueryFeatures, RequestDetail, TimestampRange, Entity, FeatureValueList
from feast.types.Value_pb2 import ValueList, TimestampList, Int32List
import feast.serving.Serving_pb2 as serving_pb

from feast.sdk.utils.types import Granularity
from feast.sdk.client import Client
from feast.sdk.importer import Importer
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.feature_group import FeatureGroup
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.storage import Storage
from feast.sdk.resources.feature_set import FeatureSet
from feast.sdk.utils.types import ServingRequestType
from feast.sdk.utils.bq_util import TrainingDatasetCreator


class TestClient(object):
    default_bq_dataset = "project.dataset"

    @pytest.fixture
    def client(self):
        return Client(core_url="some.uri", serving_url="some.serving.uri",
                      bq_dataset=self.default_bq_dataset)

    def test_apply_single_feature(self, client, mocker):
        my_feature = Feature(name="test",
                             entity="test", granularity=Granularity.NONE)
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyFeature',
                                 return_value=CoreServiceTypes.ApplyFeatureResponse(
                                     featureId="test.none.test")):
            client._core_service_stub = grpc_stub
            id = client.apply(my_feature)
            assert id == "test.none.test"

    def test_apply_single_entity(self, client, mocker):
        my_entity = Entity(name="test")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyEntity',
                                 return_value=CoreServiceTypes.ApplyEntityResponse(
                                     entityName="test")):
            client._core_service_stub = grpc_stub
            name = client.apply(my_entity)
            assert name == "test"

    def test_apply_single_feature_group(self, client, mocker):
        my_feature_group = FeatureGroup(id="test")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyFeatureGroup',
                                 return_value=CoreServiceTypes.ApplyFeatureGroupResponse(
                                     featureGroupId="test")):
            client._core_service_stub = grpc_stub
            name = client.apply(my_feature_group)
            assert name == "test"

    def test_apply_single_storage(self, client, mocker):
        my_storage = Storage(id="TEST", type="redis")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyStorage',
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

        mocker.patch.object(grpc_stub, 'ApplyStorage',
                            return_value=CoreServiceTypes.ApplyStorageResponse(
                                storageId="TEST"))
        mocker.patch.object(grpc_stub, 'ApplyFeatureGroup',
                            return_value=CoreServiceTypes.ApplyFeatureGroupResponse(
                                featureGroupId="test"))
        mocker.patch.object(grpc_stub, 'ApplyEntity',
                            return_value=CoreServiceTypes.ApplyEntityResponse(
                                entityName="test"))

        client._core_service_stub = grpc_stub
        ids = client.apply([my_storage, my_entity, my_feature_group])
        assert ids == ["TEST", "test", "test"]

    def test_run_job_no_staging(self, client, mocker):
        grpc_stub = jobs.JobServiceStub(grpc.insecure_channel(""))

        mocker.patch.object(grpc_stub, 'SubmitJob',
                            return_value=JobServiceTypes.SubmitImportJobResponse(
                                jobId="myjob12312"))
        client._job_service_stub = grpc_stub
        importer = Importer(
            {"import": ImportSpec()},
            None,
            {"require_staging": False})

        job_id = client.run(importer)
        assert job_id == "myjob12312"

    def test_create_training_dataset_invalid_args(self, client):
        feature_set = FeatureSet("entity", ["entity.none.feature1"])
        # empty feature set
        with pytest.raises(ValueError, match="feature set is empty"):
            inv_feature_set = FeatureSet("entity", [])
            client.create_training_dataset(inv_feature_set, "2018-12-01",
                                           "2018-12-02")
        # invalid start date
        with pytest.raises(ValueError, match="Incorrect date format, should be YYYY-MM-DD"):
            client.create_training_dataset(feature_set, "20181201",
                                           "2018-12-02")
        # invalid end date
        with pytest.raises(ValueError, match="Incorrect date format, should be YYYY-MM-DD"):
            client.create_training_dataset(feature_set, "2018-12-01",
                                           "20181202")
        # start date  > end date
        with pytest.raises(ValueError, match="end_date is before start_date"):
            client.create_training_dataset(feature_set, "2018-12-02",
                                           "2018-12-01")
        # invalid limit
        with pytest.raises(ValueError, match="limit is not a positive integer"):
            client.create_training_dataset(feature_set, "2018-12-01",
                                           "2018-12-02", -1)

    def test_create_training_dataset(self, client, mocker):
        bq_id = "BIGQUERY1"
        feature1 = "myentity.none.feature1"
        feature2 = "myentity.hour.feature2"
        feature1_spec = self._create_feature_spec(feature1, bq_id)
        feature2_spec = self._create_feature_spec(feature2, bq_id)
        bq_spec = self._create_bq_spec(bq_id, "project", "dataset")

        feature_set = FeatureSet("myentity", [feature1, feature2])
        start_date = "2018-12-01"
        end_date = "2018-12-05"
        limit = 1000
        destination = "project.dataset.destination"
        dataset_name = "my_training_set"

        core_stub = core.CoreServiceStub(grpc.insecure_channel(""))
        training_crt = TrainingDatasetCreator()
        mocker.patch.object(core_stub, 'GetFeatures',
                            return_value=CoreServiceTypes.GetFeaturesResponse(
                                features=[feature1_spec, feature2_spec]))
        mocker.patch.object(core_stub, 'GetStorage',
                            return_value=CoreServiceTypes.GetStorageResponse(
                                storageSpecs=[bq_spec]))
        mocker.patch.object(training_crt, "create_training_dataset",
                            return_value=Table.from_string(destination))

        client._core_service_stub = core_stub
        client._training_creator = training_crt

        ds_info = client.create_training_dataset(feature_set, start_date,
                                                 end_date, limit=limit,
                                                 destination=destination,
                                                 dataset_name=dataset_name)
        training_crt.create_training_dataset\
            .assert_called_once_with([(feature1,
                                       "project.dataset.myentity_none"),
                                      (feature2,
                                       "project.dataset.myentity_hour")],
                                     start_date,
                                     end_date,
                                     limit,
                                     destination)
        assert ds_info._table.project == "project"
        assert ds_info._table.dataset_id == "dataset"
        assert ds_info._table.table_id == "destination"

        assert ds_info.name == dataset_name

    def test_build_serving_request_last(self, client):
        feature_set = FeatureSet("entity",
                                 ["entity.none.feat1", "entity.none.feat2"])

        req = client._build_serving_request(feature_set, ["1", "2", "3"],
                                            ServingRequestType.LAST, None, None)
        expected_request_detail = \
            [RequestDetail(featureId=feat_id, type=serving_pb.LAST)
             for feat_id in ["entity.none.feat1", "entity.none.feat2"]]
        expected = QueryFeatures.Request(entityName="entity",
                                         entityId=["1", "2", "3"],
                                         requestDetails=expected_request_detail)
        assert req == expected

    def test_build_serving_request_list(self, client):
        feature_set = FeatureSet("entity",
                                 ["entity.none.feat1", "entity.none.feat2"])

        req = client._build_serving_request(
            feature_set, ["1", "2", "3"], ServingRequestType.LIST,
            [Timestamp(seconds=10), Timestamp(seconds=11)], 2)
        expected_request_detail = \
            [RequestDetail(featureId=feat_id, type=serving_pb.LIST, limit=2)
             for feat_id in ["entity.none.feat1", "entity.none.feat2"]]
        expected_ts_range = \
            TimestampRange(start=Timestamp(seconds=10), end=Timestamp(seconds=11))
        expected = QueryFeatures.Request(entityName="entity",
                                         entityId=["1", "2", "3"],
                                         requestDetails=expected_request_detail,
                                         timestampRange=expected_ts_range)
        assert req == expected

    def test_serving_response_to_df(self, client):
        response = self._create_query_features_response(
            entity_name="entity",
            entities=[{"id": "1",
                       "feat1": [(3, Timestamp(seconds=10))],
                       "feat2": [(1, Timestamp(seconds=10))]},
                      {"id": "2",
                       "feat1": [(3, Timestamp(seconds=10))],
                       "feat2": [(3, Timestamp(seconds=10))]}],
            features=["feat1", "feat2"]
        )
        df = client._response_to_df(response)
        expected_df = pd.DataFrame({'entity': ["1", "2"],
                                    'timestamp': [datetime.fromtimestamp(10),
                                                  datetime.fromtimestamp(10)],
                                    'feat1': [3, 3],
                                    'feat2': [1, 3]})
        for col in expected_df.columns:
            assert df[col].equals(expected_df[col])

    def test_serving_response_to_df_multiple(self, client):
        response = self._create_query_features_response(
            entity_name="entity",
            entities=[{"id": "1",
                       "feat1": [(3, Timestamp(seconds=10)), (4, Timestamp(seconds=11))],
                       "feat2": [(1, Timestamp(seconds=10)), (3, Timestamp(seconds=11))]},
                      {"id": "2",
                       "feat1": [(4, Timestamp(seconds=10)), (6, Timestamp(seconds=11))],
                       "feat2": [(2, Timestamp(seconds=10)), (5, Timestamp(seconds=11))]}],
            features=["feat1", "feat2"]
        )
        df = client._response_to_df(response)
        expected_df = pd.DataFrame({'entity': ["1", "1", "2", "2"],
                                    'timestamp': [datetime.fromtimestamp(10),
                                                  datetime.fromtimestamp(11),
                                                  datetime.fromtimestamp(10),
                                                  datetime.fromtimestamp(11)],
                                    'feat1': [3, 4, 4, 6],
                                    'feat2': [1, 3, 2, 5]})
        for col in expected_df.columns:
            assert df[col].equals(expected_df[col])

    def _create_query_features_response(self, entity_name, entities, features):
        # entities should be in the form:
        # {"id": "1",
        #  "feature_1": [(val1, ts1), (val2, ts2)],
        #  "feature_2": ...}
        response = QueryFeatures.Response(entityName=entity_name)
        for entity in entities:
            features_map = {}
            for feature in features:
                features_map[feature] = FeatureValueList(
                    valueList=ValueList(
                        int32List=Int32List(val=[v[0] for v in entity[feature]])
                    ),
                    timestampList=TimestampList(val=[v[1] for v in entity[feature]])
                )
            entity_pb = serving_pb.Entity(features=features_map)
            response.entities[entity["id"]].CopyFrom(entity_pb)

        return response

    def _create_feature_spec(self, feature_id, wh_id):
        wh_store = DataStore(id=wh_id)
        datastores = DataStores(warehouse=wh_store)
        return FeatureSpec(id=feature_id,
                           dataStores=datastores)

    def _create_bq_spec(self, id, project, dataset):
        return StorageSpec(id=id, type="bigquery", options={
            "project": project, "dataset": dataset})