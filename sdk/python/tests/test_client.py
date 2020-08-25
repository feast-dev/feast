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
import pkgutil
import socket
import tempfile
from concurrent import futures
from datetime import datetime
from unittest import mock

import dataframes
import grpc
import pandas as pd
import pandavro
import pytest
from google.protobuf.duration_pb2 import Duration
from mock import MagicMock, patch
from pytest_lazyfixture import lazy_fixture
from pytz import timezone

from feast.client import Client
from feast.contrib.job_controller.client import Client as JCClient
from feast.contrib.job_controller.job import IngestJob
from feast.core import CoreService_pb2_grpc as Core
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionResponse,
    GetFeatureSetResponse,
    ListFeatureSetsResponse,
    ListFeaturesResponse,
    ListIngestionJobsResponse,
)
from feast.core.FeatureSet_pb2 import EntitySpec as EntitySpecProto
from feast.core.FeatureSet_pb2 import FeatureSet as FeatureSetProto
from feast.core.FeatureSet_pb2 import FeatureSetMeta as FeatureSetMetaProto
from feast.core.FeatureSet_pb2 import FeatureSetSpec as FeatureSetSpecProto
from feast.core.FeatureSet_pb2 import FeatureSetStatus as FeatureSetStatusProto
from feast.core.FeatureSet_pb2 import FeatureSpec as FeatureSpecProto
from feast.core.IngestionJob_pb2 import IngestionJob as IngestJobProto
from feast.core.IngestionJob_pb2 import IngestionJobStatus
from feast.core.Source_pb2 import KafkaSourceConfig, Source, SourceType
from feast.core.Store_pb2 import Store
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_set import FeatureSet, FeatureSetRef
from feast.serving import ServingService_pb2_grpc as Serving
from feast.serving.ServingService_pb2 import DataFormat, FeastServingType
from feast.serving.ServingService_pb2 import FeatureReference as FeatureRefProto
from feast.serving.ServingService_pb2 import (
    GetBatchFeaturesResponse,
    GetFeastServingInfoResponse,
    GetJobResponse,
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.serving.ServingService_pb2 import Job as BatchRetrievalJob
from feast.serving.ServingService_pb2 import JobStatus, JobType
from feast.source import KafkaSource
from feast.types import Value_pb2 as ValueProto
from feast.value_type import ValueType
from feast_core_server import (
    AllowAuthInterceptor,
    CoreServicer,
    DisallowAuthInterceptor,
)
from feast_serving_server import ServingServicer

CORE_URL = "core.feast.example.com"
SERVING_URL = "serving.example.com"
jobcontroller_URL = "jobcontroller.feast.example.com"
_PRIVATE_KEY_RESOURCE_PATH = "data/localhost.key"
_CERTIFICATE_CHAIN_RESOURCE_PATH = "data/localhost.pem"
_ROOT_CERTIFICATE_RESOURCE_PATH = "data/localhost.crt"
_FAKE_JWT_TOKEN = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0N"
    "TY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDI"
    "yfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)
AUTH_METADATA = (("authorization", f"Bearer {_FAKE_JWT_TOKEN}"),)


def find_free_port():
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class TestClient:
    @pytest.fixture
    def secure_mock_client(self):
        client = Client(
            core_url=CORE_URL,
            serving_url=SERVING_URL,
            core_enable_ssl=True,
            serving_enable_ssl=True,
        )
        client._core_url = CORE_URL
        client._serving_url = SERVING_URL
        return client

    @pytest.fixture
    def mock_client(self):
        client = Client(core_url=CORE_URL, serving_url=SERVING_URL)
        client._core_url = CORE_URL
        client._serving_url = SERVING_URL
        return client

    @pytest.fixture
    def mock_jobcontroller_client(self):
        client = JCClient(jobcontroller_url=jobcontroller_URL)
        return client

    @pytest.fixture
    def mock_client_with_auth(self):
        client = Client(
            core_url=CORE_URL,
            serving_url=SERVING_URL,
            enable_auth=True,
            auth_token=_FAKE_JWT_TOKEN,
        )
        client._core_url = CORE_URL
        client._serving_url = SERVING_URL
        return client

    @pytest.fixture
    def secure_mock_client_with_auth(self):
        client = Client(
            core_url=CORE_URL,
            serving_url=SERVING_URL,
            core_enable_ssl=True,
            serving_enable_ssl=True,
            enable_auth=True,
            auth_token=_FAKE_JWT_TOKEN,
        )
        client._core_url = CORE_URL
        client._serving_url = SERVING_URL
        return client

    @pytest.fixture
    def server_credentials(self):
        private_key = pkgutil.get_data(__name__, _PRIVATE_KEY_RESOURCE_PATH)
        certificate_chain = pkgutil.get_data(__name__, _CERTIFICATE_CHAIN_RESOURCE_PATH)
        return grpc.ssl_server_credentials(((private_key, certificate_chain),))

    @pytest.fixture
    def core_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        port = find_free_port()
        server.add_insecure_port(f"[::]:{port}")
        server.start()
        yield port
        server.stop(0)

    @pytest.fixture
    def serving_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Serving.add_ServingServiceServicer_to_server(ServingServicer(), server)
        port = find_free_port()
        server.add_insecure_port(f"[::]:{port}")
        server.start()
        yield port
        server.stop(0)

    @pytest.fixture
    def secure_core_server(self, server_credentials):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        port = find_free_port()
        server.add_secure_port(f"[::]:{port}", server_credentials)
        server.start()
        yield port
        server.stop(0)

    @pytest.fixture
    def secure_serving_server(self, server_credentials):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Serving.add_ServingServiceServicer_to_server(ServingServicer(), server)
        port = find_free_port()
        server.add_secure_port(f"[::]:{port}", server_credentials)
        server.start()
        yield port
        server.stop(0)

    @pytest.fixture
    def secure_core_server_with_auth(self, server_credentials):
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=(AllowAuthInterceptor(),),
        )
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        port = find_free_port()
        server.add_secure_port(f"[::]:{port}", server_credentials)
        server.start()
        yield port
        server.stop(0)

    @pytest.fixture
    def insecure_core_server_with_auth(self, server_credentials):
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=(AllowAuthInterceptor(),),
        )
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        port = find_free_port()
        server.add_insecure_port(f"[::]:{port}")
        server.start()
        yield port
        server.stop(0)

    @pytest.fixture
    def insecure_core_server_that_blocks_auth(self, server_credentials):
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=(DisallowAuthInterceptor(),),
        )
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        port = find_free_port()
        server.add_insecure_port(f"[::]:{port}")
        server.start()
        yield port
        server.stop(0)

    @pytest.fixture
    def secure_client(self, secure_core_server, secure_serving_server):
        root_certificate_credentials = pkgutil.get_data(
            __name__, _ROOT_CERTIFICATE_RESOURCE_PATH
        )

        ssl_channel_credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificate_credentials
        )
        with mock.patch(
            "grpc.ssl_channel_credentials",
            MagicMock(return_value=ssl_channel_credentials),
        ):
            yield Client(
                core_url=f"localhost:{secure_core_server}",
                serving_url=f"localhost:{secure_serving_server}",
                core_enable_ssl=True,
                serving_enable_ssl=True,
            )

    @pytest.fixture
    def secure_core_client_with_auth(self, secure_core_server_with_auth):
        root_certificate_credentials = pkgutil.get_data(
            __name__, _ROOT_CERTIFICATE_RESOURCE_PATH
        )
        ssl_channel_credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificate_credentials
        )
        with mock.patch(
            "grpc.ssl_channel_credentials",
            MagicMock(return_value=ssl_channel_credentials),
        ):
            yield Client(
                core_url=f"localhost:{secure_core_server_with_auth}",
                core_enable_ssl=True,
                enable_auth=True,
                auth_token=_FAKE_JWT_TOKEN,
            )

    @pytest.fixture
    def client(self, core_server, serving_server):
        return Client(
            core_url=f"localhost:{core_server}",
            serving_url=f"localhost:{serving_server}",
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_version(self, mocked_client, mocker):
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )
        mocked_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeastCoreVersion",
            return_value=GetFeastCoreVersionResponse(version="0.3.2"),
        )

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetFeastServingInfo",
            return_value=GetFeastServingInfoResponse(version="0.3.2"),
        )

        status = mocked_client.version()
        assert (
            status["core"]["url"] == CORE_URL
            and status["core"]["version"] == "0.3.2"
            and status["serving"]["url"] == SERVING_URL
            and status["serving"]["version"] == "0.3.2"
        )

    @pytest.mark.parametrize(
        "mocked_client,auth_metadata",
        [
            (lazy_fixture("mock_client"), ()),
            (lazy_fixture("mock_client_with_auth"), (AUTH_METADATA)),
            (lazy_fixture("secure_mock_client"), ()),
            (lazy_fixture("secure_mock_client_with_auth"), (AUTH_METADATA)),
        ],
        ids=[
            "mock_client_without_auth",
            "mock_client_with_auth",
            "secure_mock_client_without_auth",
            "secure_mock_client_with_auth",
        ],
    )
    def test_get_online_features(self, mocked_client, auth_metadata, mocker):
        ROW_COUNT = 300

        mocked_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )

        def int_val(x):
            return ValueProto.Value(int64_val=x)

        request = GetOnlineFeaturesRequest(project="driver_project")
        request.features.extend(
            [
                FeatureRefProto(feature_set="driver", name="age"),
                FeatureRefProto(name="rating"),
                FeatureRefProto(name="null_value"),
            ]
        )
        recieve_response = GetOnlineFeaturesResponse()
        for row_number in range(1, ROW_COUNT + 1):
            request.entity_rows.append(
                GetOnlineFeaturesRequest.EntityRow(
                    fields={"driver_id": int_val(row_number)}
                )
            ),
            field_values = GetOnlineFeaturesResponse.FieldValues(
                fields={
                    "driver_id": int_val(row_number),
                    "driver:age": int_val(1),
                    "rating": int_val(9),
                    "null_value": ValueProto.Value(),
                },
                statuses={
                    "driver_id": GetOnlineFeaturesResponse.FieldStatus.PRESENT,
                    "driver:age": GetOnlineFeaturesResponse.FieldStatus.PRESENT,
                    "rating": GetOnlineFeaturesResponse.FieldStatus.PRESENT,
                    "null_value": GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE,
                },
            )
            recieve_response.field_values.append(field_values)

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetOnlineFeatures",
            return_value=recieve_response,
        )
        got_response = mocked_client.get_online_features(
            entity_rows=request.entity_rows,
            feature_refs=["driver:age", "rating", "null_value"],
            project="driver_project",
        )  # type: GetOnlineFeaturesResponse
        mocked_client._serving_service_stub.GetOnlineFeatures.assert_called_with(
            request, metadata=auth_metadata
        )

        got_fields = got_response.field_values[0].fields
        got_statuses = got_response.field_values[0].statuses
        assert (
            got_fields["driver_id"] == int_val(1)
            and got_statuses["driver_id"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver:age"] == int_val(1)
            and got_statuses["driver:age"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["rating"] == int_val(9)
            and got_statuses["rating"] == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["null_value"] == ValueProto.Value()
            and got_statuses["null_value"]
            == GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_get_feature_set(self, mocked_client, mocker):
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )

        from google.protobuf.duration_pb2 import Duration

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(
                feature_set=FeatureSetProto(
                    spec=FeatureSetSpecProto(
                        name="my_feature_set",
                        max_age=Duration(seconds=3600),
                        labels={"key1": "val1", "key2": "val2"},
                        features=[
                            FeatureSpecProto(
                                name="my_feature_1",
                                value_type=ValueProto.ValueType.FLOAT,
                            ),
                            FeatureSpecProto(
                                name="my_feature_2",
                                value_type=ValueProto.ValueType.FLOAT,
                            ),
                        ],
                        entities=[
                            EntitySpecProto(
                                name="my_entity_1",
                                value_type=ValueProto.ValueType.INT64,
                            )
                        ],
                        source=Source(
                            type=SourceType.KAFKA,
                            kafka_source_config=KafkaSourceConfig(
                                bootstrap_servers="localhost:9092", topic="topic"
                            ),
                        ),
                    ),
                    meta=FeatureSetMetaProto(),
                )
            ),
        )
        mocked_client.set_project("my_project")
        feature_set = mocked_client.get_feature_set("my_feature_set")

        assert (
            feature_set.name == "my_feature_set"
            and "key1" in feature_set.labels
            and feature_set.labels["key1"] == "val1"
            and "key2" in feature_set.labels
            and feature_set.labels["key2"] == "val2"
            and feature_set.fields["my_feature_1"].name == "my_feature_1"
            and feature_set.fields["my_feature_1"].dtype == ValueType.FLOAT
            and feature_set.fields["my_entity_1"].name == "my_entity_1"
            and feature_set.fields["my_entity_1"].dtype == ValueType.INT64
            and len(feature_set.features) == 2
            and len(feature_set.entities) == 1
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_list_feature_sets(self, mocked_client, mocker):
        mocker.patch.object(
            mocked_client,
            "_core_service_stub",
            return_value=Core.CoreServiceStub(grpc.insecure_channel("")),
        )

        feature_set_1_proto = FeatureSetProto(
            spec=FeatureSetSpecProto(
                project="test",
                name="driver_car",
                max_age=Duration(seconds=3600),
                labels={"key1": "val1", "key2": "val2"},
                features=[
                    FeatureSpecProto(
                        name="feature_1", value_type=ValueProto.ValueType.FLOAT
                    )
                ],
            )
        )
        feature_set_2_proto = FeatureSetProto(
            spec=FeatureSetSpecProto(
                project="test",
                name="driver_ride",
                max_age=Duration(seconds=3600),
                labels={"key1": "val1"},
                features=[
                    FeatureSpecProto(
                        name="feature_1", value_type=ValueProto.ValueType.FLOAT
                    )
                ],
            )
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "ListFeatureSets",
            return_value=ListFeatureSetsResponse(
                feature_sets=[feature_set_1_proto, feature_set_2_proto]
            ),
        )

        feature_sets = mocked_client.list_feature_sets(labels={"key1": "val1"})
        assert len(feature_sets) == 2

        feature_set = feature_sets[0]
        assert (
            feature_set.name == "driver_car"
            and "key1" in feature_set.labels
            and feature_set.labels["key1"] == "val1"
            and "key2" in feature_set.labels
            and feature_set.labels["key2"] == "val2"
            and feature_set.fields["feature_1"].name == "feature_1"
            and feature_set.fields["feature_1"].dtype == ValueType.FLOAT
            and len(feature_set.features) == 1
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_list_features(self, mocked_client, mocker):
        mocker.patch.object(
            mocked_client,
            "_core_service_stub",
            return_value=Core.CoreServiceStub(grpc.insecure_channel("")),
        )

        feature1_proto = FeatureSpecProto(
            name="feature_1", value_type=ValueProto.ValueType.FLOAT
        )
        feature2_proto = FeatureSpecProto(
            name="feature_2", value_type=ValueProto.ValueType.STRING
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "ListFeatures",
            return_value=ListFeaturesResponse(
                features={
                    "driver_car:feature_1": feature1_proto,
                    "driver_car:feature_2": feature2_proto,
                }
            ),
        )

        features = mocked_client.list_features_by_ref(project="test")
        assert len(features) == 2

        ref_str_list = []
        feature_name_list = []
        feature_dtype_list = []
        for ref_str, feature_proto in features.items():
            ref_str_list.append(ref_str)
            feature_name_list.append(feature_proto.name)
            feature_dtype_list.append(feature_proto.dtype)

        assert (
            set(ref_str_list) == set(["driver_car:feature_1", "driver_car:feature_2"])
            and set(feature_name_list) == set(["feature_1", "feature_2"])
            and set(feature_dtype_list) == set([ValueType.FLOAT, ValueType.STRING])
        )

    def test_list_ingest_jobs(self, mock_jobcontroller_client, mocker):
        mocker.patch.object(
            mock_jobcontroller_client,
            "_jobcontroller_service_stub",
            return_value=Core.JobControllerServiceStub(grpc.insecure_channel("")),
        )

        feature_set_ref = FeatureSetRef(project="test", name="driver",)

        mocker.patch.object(
            mock_jobcontroller_client._jobcontroller_service_stub,
            "ListIngestionJobs",
            return_value=ListIngestionJobsResponse(
                jobs=[
                    IngestJobProto(
                        id="kafka-to-redis",
                        external_id="job-2222",
                        status=IngestionJobStatus.RUNNING,
                        feature_set_references=[feature_set_ref.to_proto()],
                        source=Source(
                            type=SourceType.KAFKA,
                            kafka_source_config=KafkaSourceConfig(
                                bootstrap_servers="localhost:9092", topic="topic"
                            ),
                        ),
                        stores=[Store(name="redis")],
                    )
                ]
            ),
        )

        # list ingestion jobs by target feature set reference
        ingest_jobs = mock_jobcontroller_client.list_ingest_jobs(
            feature_set_ref=feature_set_ref
        )
        assert len(ingest_jobs) >= 1

        ingest_job = ingest_jobs[0]
        assert (
            ingest_job.status == IngestionJobStatus.RUNNING
            and ingest_job.id == "kafka-to-redis"
            and ingest_job.external_id == "job-2222"
            and ingest_job.feature_sets[0].name == "driver"
            and ingest_job.source.source_type == "Kafka"
        )

    def test_restart_ingest_job(self, mock_jobcontroller_client, mocker):
        mocker.patch.object(
            mock_jobcontroller_client,
            "_jobcontroller_service_stub",
            return_value=Core.JobControllerServiceStub(grpc.insecure_channel("")),
        )

        ingest_job = IngestJob(
            job_proto=IngestJobProto(
                id="kafka-to-redis",
                external_id="job#2222",
                status=IngestionJobStatus.ERROR,
            ),
            core_stub=mock_jobcontroller_client._jobcontroller_service_stub,
        )

        mock_jobcontroller_client.restart_ingest_job(ingest_job)
        assert (
            mock_jobcontroller_client._jobcontroller_service_stub.RestartIngestionJob.called
        )

    def test_stop_ingest_job(self, mock_jobcontroller_client, mocker):
        mocker.patch.object(
            mock_jobcontroller_client,
            "_jobcontroller_service_stub",
            return_value=Core.JobControllerServiceStub(grpc.insecure_channel("")),
        )

        ingest_job = IngestJob(
            job_proto=IngestJobProto(
                id="kafka-to-redis",
                external_id="job#2222",
                status=IngestionJobStatus.RUNNING,
            ),
            core_stub=mock_jobcontroller_client._jobcontroller_service_stub,
        )

        mock_jobcontroller_client.stop_ingest_job(ingest_job)
        assert (
            mock_jobcontroller_client._jobcontroller_service_stub.StopIngestionJob.called
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [
            lazy_fixture("mock_client"),
            lazy_fixture("mock_client_with_auth"),
            lazy_fixture("secure_mock_client"),
            lazy_fixture("secure_mock_client_with_auth"),
        ],
    )
    def test_get_historical_features(self, mocked_client, mocker):

        mocked_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(
                feature_set=FeatureSetProto(
                    spec=FeatureSetSpecProto(
                        name="driver",
                        project="driver_project",
                        entities=[
                            EntitySpecProto(
                                name="driver", value_type=ValueProto.ValueType.INT64
                            ),
                            EntitySpecProto(
                                name="transaction",
                                value_type=ValueProto.ValueType.INT64,
                            ),
                        ],
                        features=[
                            FeatureSpecProto(
                                name="driver_id", value_type=ValueProto.ValueType.FLOAT,
                            ),
                            FeatureSpecProto(
                                name="driver_name",
                                value_type=ValueProto.ValueType.STRING,
                            ),
                        ],
                    ),
                    meta=FeatureSetMetaProto(status=FeatureSetStatusProto.STATUS_READY),
                )
            ),
        )

        expected_dataframe = pd.DataFrame(
            {
                "datetime": [datetime.utcnow() for _ in range(3)],
                "driver": [1001, 1002, 1003],
                "transaction": [1001, 1002, 1003],
                "driver_id": [1001, 1002, 1003],
            }
        )

        final_results = tempfile.mktemp()
        pandavro.to_avro(file_path_or_buffer=final_results, df=expected_dataframe)

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetBatchFeatures",
            return_value=GetBatchFeaturesResponse(
                job=BatchRetrievalJob(
                    id="123",
                    type=JobType.JOB_TYPE_DOWNLOAD,
                    status=JobStatus.JOB_STATUS_DONE,
                    file_uris=[f"file://{final_results}"],
                    data_format=DataFormat.DATA_FORMAT_AVRO,
                )
            ),
        )

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetJob",
            return_value=GetJobResponse(
                job=BatchRetrievalJob(
                    id="123",
                    type=JobType.JOB_TYPE_DOWNLOAD,
                    status=JobStatus.JOB_STATUS_DONE,
                    file_uris=[f"file://{final_results}"],
                    data_format=DataFormat.DATA_FORMAT_AVRO,
                )
            ),
        )

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetFeastServingInfo",
            return_value=GetFeastServingInfoResponse(
                job_staging_location=f"file://{tempfile.mkdtemp()}/",
                type=FeastServingType.FEAST_SERVING_TYPE_BATCH,
            ),
        )

        mocked_client.set_project("project1")
        # TODO: Abstract away GCS client and GCP dependency
        # NOTE: Feast Serving does not allow for feature references
        # that specify the same feature in the same request.
        with patch("google.cloud.storage.Client"):
            response = mocked_client.get_historical_features(
                entity_rows=pd.DataFrame(
                    {
                        "datetime": [
                            pd.datetime.now(tz=timezone("Asia/Singapore"))
                            for _ in range(3)
                        ],
                        "driver": [1001, 1002, 1003],
                        "transaction": [1001, 1002, 1003],
                    }
                ),
                feature_refs=["driver:driver_id", "driver_id"],
                project="driver_project",
            )  # Type: GetBatchFeaturesResponse

        assert response.id == "123" and response.status == JobStatus.JOB_STATUS_DONE

        actual_dataframe = response.to_dataframe()

        assert actual_dataframe[["driver_id"]].equals(expected_dataframe[["driver_id"]])

    @pytest.mark.parametrize(
        "test_client", [lazy_fixture("client"), lazy_fixture("secure_client")],
    )
    def test_apply_feature_set_success(self, test_client):

        test_client.set_project("project1")

        # Create Feature Sets
        fs1 = FeatureSet("my-feature-set-1")
        fs1.add(Feature(name="fs1-my-feature-1", dtype=ValueType.INT64))
        fs1.add(Feature(name="fs1-my-feature-2", dtype=ValueType.STRING))
        fs1.add(Entity(name="fs1-my-entity-1", dtype=ValueType.INT64))

        fs2 = FeatureSet("my-feature-set-2")
        fs2.add(Feature(name="fs2-my-feature-1", dtype=ValueType.STRING_LIST))
        fs2.add(Feature(name="fs2-my-feature-2", dtype=ValueType.BYTES_LIST))
        fs2.add(Entity(name="fs2-my-entity-1", dtype=ValueType.INT64))

        # Register Feature Set with Core
        test_client.apply(fs1)
        test_client.apply(fs2)

        feature_sets = test_client.list_feature_sets()

        # List Feature Sets
        assert (
            len(feature_sets) == 2
            and feature_sets[0].name == "my-feature-set-1"
            and feature_sets[0].features[0].name == "fs1-my-feature-1"
            and feature_sets[0].features[0].dtype == ValueType.INT64
            and feature_sets[0].features[1].name == "fs1-my-feature-2"
            and feature_sets[0].features[1].dtype == ValueType.STRING
            and feature_sets[0].entities[0].name == "fs1-my-entity-1"
            and feature_sets[0].entities[0].dtype == ValueType.INT64
            and feature_sets[1].features[0].name == "fs2-my-feature-1"
            and feature_sets[1].features[0].dtype == ValueType.STRING_LIST
            and feature_sets[1].features[1].name == "fs2-my-feature-2"
            and feature_sets[1].features[1].dtype == ValueType.BYTES_LIST
            and feature_sets[1].entities[0].name == "fs2-my-entity-1"
            and feature_sets[1].entities[0].dtype == ValueType.INT64
        )

    @pytest.mark.parametrize(
        "dataframe,test_client",
        [
            (dataframes.GOOD, lazy_fixture("client")),
            (dataframes.GOOD, lazy_fixture("secure_client")),
        ],
    )
    def test_feature_set_ingest_success(self, dataframe, test_client, mocker):
        test_client.set_project("project1")
        driver_fs = FeatureSet(
            "driver-feature-set", source=KafkaSource(brokers="kafka:9092", topic="test")
        )
        driver_fs.add(Feature(name="feature_1", dtype=ValueType.FLOAT))
        driver_fs.add(Feature(name="feature_2", dtype=ValueType.STRING))
        driver_fs.add(Feature(name="feature_3", dtype=ValueType.INT64))
        driver_fs.add(Entity(name="entity_id", dtype=ValueType.INT64))

        # Register with Feast core
        test_client.apply(driver_fs)
        driver_fs = driver_fs.to_proto()
        driver_fs.meta.status = FeatureSetStatusProto.STATUS_READY

        mocker.patch.object(
            test_client._core_service_stub,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(feature_set=driver_fs),
        )

        # Need to create a mock producer
        with patch("feast.client.get_producer"):
            # Ingest data into Feast
            test_client.ingest("driver-feature-set", dataframe)

    @pytest.mark.parametrize(
        "dataframe,test_client,exception",
        [(dataframes.GOOD, lazy_fixture("client"), Exception)],
    )
    def test_feature_set_ingest_throws_exception_if_kafka_down(
        self, dataframe, test_client, exception, mocker
    ):

        test_client.set_project("project1")
        driver_fs = FeatureSet(
            "driver-feature-set",
            source=KafkaSource(brokers="localhost:4412", topic="test"),
        )
        driver_fs.add(Feature(name="feature_1", dtype=ValueType.FLOAT))
        driver_fs.add(Feature(name="feature_2", dtype=ValueType.STRING))
        driver_fs.add(Feature(name="feature_3", dtype=ValueType.INT64))
        driver_fs.add(Entity(name="entity_id", dtype=ValueType.INT64))

        # Register with Feast core
        test_client.apply(driver_fs)
        driver_fs = driver_fs.to_proto()
        driver_fs.meta.status = FeatureSetStatusProto.STATUS_READY

        mocker.patch.object(
            test_client._core_service_stub,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(feature_set=driver_fs),
        )

        with pytest.raises(exception):
            test_client.ingest("driver-feature-set", dataframe, timeout=1)

    @pytest.mark.parametrize(
        "dataframe,exception,test_client",
        [
            (dataframes.GOOD, TimeoutError, lazy_fixture("client")),
            (dataframes.GOOD, TimeoutError, lazy_fixture("secure_client")),
        ],
    )
    def test_feature_set_ingest_fail_if_pending(
        self, dataframe, exception, test_client, mocker
    ):
        with pytest.raises(exception):
            test_client.set_project("project1")
            driver_fs = FeatureSet(
                "driver-feature-set",
                source=KafkaSource(brokers="kafka:9092", topic="test"),
            )
            driver_fs.add(Feature(name="feature_1", dtype=ValueType.FLOAT))
            driver_fs.add(Feature(name="feature_2", dtype=ValueType.STRING))
            driver_fs.add(Feature(name="feature_3", dtype=ValueType.INT64))
            driver_fs.add(Entity(name="entity_id", dtype=ValueType.INT64))

            # Register with Feast core
            test_client.apply(driver_fs)
            driver_fs = driver_fs.to_proto()
            driver_fs.meta.status = FeatureSetStatusProto.STATUS_PENDING

            mocker.patch.object(
                test_client._core_service_stub,
                "GetFeatureSet",
                return_value=GetFeatureSetResponse(feature_set=driver_fs),
            )

            # Need to create a mock producer
            with patch("feast.client.get_producer"):
                # Ingest data into Feast
                test_client.ingest("driver-feature-set", dataframe, timeout=1)

    @pytest.mark.parametrize(
        "dataframe,exception,test_client",
        [
            (dataframes.BAD_NO_DATETIME, Exception, lazy_fixture("client")),
            (
                dataframes.BAD_INCORRECT_DATETIME_TYPE,
                Exception,
                lazy_fixture("client"),
            ),
            (dataframes.BAD_NO_ENTITY, Exception, lazy_fixture("client")),
            (dataframes.NO_FEATURES, Exception, lazy_fixture("client")),
            (dataframes.BAD_NO_DATETIME, Exception, lazy_fixture("secure_client"),),
            (
                dataframes.BAD_INCORRECT_DATETIME_TYPE,
                Exception,
                lazy_fixture("secure_client"),
            ),
            (dataframes.BAD_NO_ENTITY, Exception, lazy_fixture("secure_client")),
            (dataframes.NO_FEATURES, Exception, lazy_fixture("secure_client")),
        ],
    )
    def test_feature_set_ingest_failure(self, test_client, dataframe, exception):
        with pytest.raises(exception):
            # Create feature set
            driver_fs = FeatureSet("driver-feature-set")

            # Update based on dataset
            driver_fs.infer_fields_from_df(dataframe)

            # Register with Feast core
            test_client.apply(driver_fs)

            # Ingest data into Feast
            test_client.ingest(driver_fs, dataframe=dataframe)

    @pytest.mark.parametrize(
        "dataframe,test_client",
        [
            (dataframes.ALL_TYPES, lazy_fixture("client")),
            (dataframes.ALL_TYPES, lazy_fixture("secure_client")),
        ],
    )
    def test_feature_set_types_success(self, test_client, dataframe, mocker):

        test_client.set_project("project1")

        all_types_fs = FeatureSet(
            name="all_types",
            entities=[Entity(name="user_id", dtype=ValueType.INT64)],
            features=[
                Feature(name="float_feature", dtype=ValueType.FLOAT),
                Feature(name="int64_feature", dtype=ValueType.INT64),
                Feature(name="int32_feature", dtype=ValueType.INT32),
                Feature(name="string_feature", dtype=ValueType.STRING),
                Feature(name="bytes_feature", dtype=ValueType.BYTES),
                Feature(name="bool_feature", dtype=ValueType.BOOL),
                Feature(name="double_feature", dtype=ValueType.DOUBLE),
                Feature(name="float_list_feature", dtype=ValueType.FLOAT_LIST),
                Feature(name="int64_list_feature", dtype=ValueType.INT64_LIST),
                Feature(name="int32_list_feature", dtype=ValueType.INT32_LIST),
                Feature(name="string_list_feature", dtype=ValueType.STRING_LIST),
                Feature(name="bytes_list_feature", dtype=ValueType.BYTES_LIST),
                # Feature(name="bool_list_feature",
                # dtype=ValueType.BOOL_LIST), # TODO: Add support for this
                #  type again https://github.com/feast-dev/feast/issues/341
                Feature(name="double_list_feature", dtype=ValueType.DOUBLE_LIST),
            ],
            max_age=Duration(seconds=3600),
        )

        # Register with Feast core
        test_client.apply(all_types_fs)

        mocker.patch.object(
            test_client._core_service,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(feature_set=all_types_fs.to_proto()),
        )

        # Need to create a mock producer
        with patch("feast.client.get_producer"):
            # Ingest data into Feast
            test_client.ingest(all_types_fs, dataframe)

    @patch("grpc.channel_ready_future")
    def test_secure_channel_creation_with_secure_client(
        self, _mocked_obj, core_server, serving_server
    ):
        client = Client(
            core_url=f"localhost:{core_server}",
            serving_url=f"localhost:{serving_server}",
            serving_enable_ssl=True,
            core_enable_ssl=True,
        )
        with mock.patch("grpc.secure_channel") as _grpc_mock, mock.patch(
            "grpc.ssl_channel_credentials", MagicMock(return_value="test")
        ) as _mocked_credentials:
            _ = client._serving_service
            _grpc_mock.assert_called_with(
                client.serving_url, credentials=_mocked_credentials.return_value
            )

    @mock.patch("grpc.channel_ready_future")
    def test_secure_channel_creation_with_secure_serving_url(
        self, _mocked_obj, core_server
    ):
        client = Client(
            core_url=f"localhost:{core_server}", serving_url="localhost:443"
        )
        with mock.patch("grpc.secure_channel") as _grpc_mock, mock.patch(
            "grpc.ssl_channel_credentials", MagicMock(return_value="test")
        ) as _mocked_credentials:
            _ = client._serving_service
            _grpc_mock.assert_called_with(
                client.serving_url, credentials=_mocked_credentials.return_value
            )

    @patch("grpc.channel_ready_future")
    def test_secure_channel_creation_with_secure_core_url(
        self, _mocked_obj, secure_serving_server
    ):
        client = Client(
            core_url="localhost:443", serving_url=f"localhost:{secure_serving_server}",
        )
        with mock.patch("grpc.secure_channel") as _grpc_mock, mock.patch(
            "grpc.ssl_channel_credentials", MagicMock(return_value="test")
        ) as _mocked_credentials:
            _ = client._core_service
            _grpc_mock.assert_called_with(
                client.core_url, credentials=_mocked_credentials.return_value
            )

    @mock.patch("grpc.channel_ready_future")
    def test_auth_success_with_secure_channel_on_core_url(
        self, secure_core_client_with_auth
    ):
        secure_core_client_with_auth.list_feature_sets()

    def test_auth_success_with_insecure_channel_on_core_url(
        self, insecure_core_server_with_auth
    ):
        client = Client(
            core_url=f"localhost:{insecure_core_server_with_auth}",
            enable_auth=True,
            auth_token=_FAKE_JWT_TOKEN,
        )
        client.list_feature_sets()

    def test_no_auth_sent_when_auth_disabled(
        self, insecure_core_server_that_blocks_auth
    ):
        client = Client(core_url=f"localhost:{insecure_core_server_that_blocks_auth}")
        client.list_feature_sets()
