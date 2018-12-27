import pytest
from unittest.mock import patch

import grpc

import feast.core.CoreService_pb2_grpc as core
import feast.core.JobService_pb2_grpc as jobs
import feast.specs.StorageSpec_pb2 as storage_pb
from feast.core.CoreService_pb2 import CoreServiceTypes
from feast.core.JobService_pb2 import JobServiceTypes
from feast.sdk.utils.types import Granularity
from feast.specs.ImportSpec_pb2 import ImportSpec
from feast.sdk.client import Client
from feast.sdk.importer import Importer
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.feature_group import FeatureGroup
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.storage import Storage


@pytest.fixture
def client():
    return Client("some.uri")


class TestClient(object):
    def test_apply_single_feature(self, client, mocker):
        my_feature = Feature(name="test",
                             entity="test", granularity=Granularity.NONE)
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyFeature',
                                 return_value=CoreServiceTypes.ApplyFeatureResponse(
                                     featureId="test.none.test")):
            client.core_service_stub = grpc_stub
            id = client.apply(my_feature)
            assert id == "test.none.test"

    def test_apply_single_entity(self, client, mocker):
        my_entity = Entity(name="test")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyEntity',
                                 return_value=CoreServiceTypes.ApplyEntityResponse(
                                     entityName="test")):
            client.core_service_stub = grpc_stub
            name = client.apply(my_entity)
            assert name == "test"

    def test_apply_single_feature_group(self, client, mocker):
        my_feature_group = FeatureGroup(id="test")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyFeatureGroup',
                                 return_value=CoreServiceTypes.ApplyFeatureGroupResponse(
                                     featureGroupId="test")):
            client.core_service_stub = grpc_stub
            name = client.apply(my_feature_group)
            assert name == "test"

    def test_apply_single_storage(self, client, mocker):
        my_storage = Storage(id="TEST", type="redis")
        grpc_stub = core.CoreServiceStub(grpc.insecure_channel(""))

        with mocker.patch.object(grpc_stub, 'ApplyStorage',
                                 return_value=CoreServiceTypes.ApplyStorageResponse(
                                     storageId="TEST")):
            client.core_service_stub = grpc_stub
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

        client.core_service_stub = grpc_stub
        ids = client.apply([my_storage, my_entity, my_feature_group])
        assert ids == ["TEST", "test", "test"]

    def test_run_job_no_staging(self, client, mocker):
        grpc_stub = jobs.JobServiceStub(grpc.insecure_channel(""))

        mocker.patch.object(grpc_stub, 'SubmitJob',
                            return_value=JobServiceTypes.SubmitImportJobResponse(
                                jobId="myjob12312"))
        client.job_service_stub = grpc_stub
        importer = Importer(
            {"import": ImportSpec()},
            None,
            {"require_staging": False})

        job_id = client.run(importer)
        assert job_id == "myjob12312"

    # def test_create_feature_set(self, client, mocker):
    #     entity_name = "myentity"
    #     granularity = Granularity.DAY
    #     features = ["feature1", "feature2", "feature3"]
    #     wh_store_id = "BIGQUERY1"
    #     project = "test_project"
    #     dataset = "test_dataset"
    #
    #     core_stub = core.CoreServiceStub(grpc.insecure_channel(""))
    #     feature_specs = self._create_feature_specs(entity_name, granularity,
    #                                                features, wh_store_id)
    #     storage_specs = self._create_bq_spec(wh_store_id, project, dataset)
    #     mocker.patch.object(core_stub, 'GetFeatures',
    #                         return_value=feature_specs)
    #     mocker.patch.object(core_stub, 'GetStorage',
    #                         return_value=storage_specs)
    #
    #     client.create_feature_set(entity_name,
    #                               granularity,
    #                               features)

    def _create_feature_specs(self, entity, granularity, features, wh_id):
        return

    def _create_bq_spec(self, id, project, dataset):
        return storage_pb.StorageSpec(id=id, type="bigquery", options={
            "project": project, "dataset": dataset})

    # def test_run_job_require_staging(self, client, mocker):
    #     importer = Importer()
    #     mocker.patch.object(importer,
