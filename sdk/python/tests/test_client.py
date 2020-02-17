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
from datetime import datetime

import tempfile
from unittest import mock

import grpc
import pandas as pd
from google.protobuf.duration_pb2 import Duration
from mock import MagicMock, patch
from pytz import timezone
from pandavro import to_avro
import feast.core.CoreService_pb2_grpc as Core
import feast.serving.ServingService_pb2_grpc as Serving
from feast.feature_set import FeatureSet
from feast.entity import Entity
from feast.feature_set import Feature
from feast.source import KafkaSource
from feast.core.FeatureSet_pb2 import (
    FeatureSetSpec as FeatureSetSpecProto,
    FeatureSpec as FeatureSpecProto,
    EntitySpec as EntitySpecProto,
    FeatureSetMeta as FeatureSetMetaProto,
    FeatureSetStatus as FeatureSetStatusProto,
    FeatureSet as FeatureSetProto,
)
from feast.core.Source_pb2 import SourceType, KafkaSourceConfig, Source
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionResponse,
    GetFeatureSetResponse,
)
from feast.serving.ServingService_pb2 import (
    GetFeastServingInfoResponse,
    GetOnlineFeaturesResponse,
    GetOnlineFeaturesRequest,
    GetBatchFeaturesResponse,
    Job as BatchFeaturesJob,
    JobType,
    JobStatus,
    DataFormat,
    GetJobResponse,
    FeastServingType,
)
import pytest
from feast.client import Client
from concurrent import futures
from feast_core_server import CoreServicer
from feast_serving_server import ServingServicer
from feast.types import Value_pb2 as ValueProto
from feast.value_type import ValueType
from feast.job import Job
import dataframes

CORE_URL = "core.feast.example.com"
SERVING_URL = "serving.example.com"
_PRIVATE_KEY_RESOURCE_PATH = 'data/localhost.key'
_CERTIFICATE_CHAIN_RESOURCE_PATH = 'data/localhost.pem'
_ROOT_CERTIFICATE_RESOURCE_PATH = 'data/localhost.crt'


class TestClient:

    @pytest.fixture
    def secure_mock_client(self, mocker):
        client = Client(core_url=CORE_URL, serving_url=SERVING_URL, core_secure=True, serving_secure=True)
        mocker.patch.object(client, "_connect_core")
        mocker.patch.object(client, "_connect_serving")
        client._core_url = CORE_URL
        client._serving_url = SERVING_URL
        return client

    @pytest.fixture
    def mock_client(self, mocker):
        client = Client(core_url=CORE_URL, serving_url=SERVING_URL)
        mocker.patch.object(client, "_connect_core")
        mocker.patch.object(client, "_connect_serving")
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
        server.add_insecure_port("[::]:50051")
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def serving_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Serving.add_ServingServiceServicer_to_server(ServingServicer(), server)
        server.add_insecure_port("[::]:50052")
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def secure_core_server(self, server_credentials):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        server.add_secure_port("[::]:50053", server_credentials)
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def secure_serving_server(self, server_credentials):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Serving.add_ServingServiceServicer_to_server(ServingServicer(), server)

        server.add_secure_port("[::]:50054", server_credentials)
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def secure_client(self, secure_core_server, secure_serving_server):
        root_certificate_credentials = pkgutil.get_data(__name__, _ROOT_CERTIFICATE_RESOURCE_PATH)
        # this is needed to establish a secure connection using self-signed certificates, for the purpose of the test
        ssl_channel_credentials = grpc.ssl_channel_credentials(root_certificates=root_certificate_credentials)
        with mock.patch("grpc.ssl_channel_credentials", MagicMock(return_value=ssl_channel_credentials)):
            yield Client(core_url="localhost:50053", serving_url="localhost:50054", core_secure=True,
                         serving_secure=True)

    @pytest.fixture
    def client(self, core_server, serving_server):
        return Client(core_url="localhost:50051", serving_url="localhost:50052")

    @pytest.mark.parametrize("mocked_client", [pytest.lazy_fixture("mock_client"),
                                               pytest.lazy_fixture("secure_mock_client")
                                               ])
    def test_version(self, mocked_client, mocker):
        mocked_client._core_service_stub = Core.CoreServiceStub(grpc.insecure_channel(""))
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

    @pytest.mark.parametrize("mocked_client", [pytest.lazy_fixture("mock_client"),
                                               pytest.lazy_fixture("secure_mock_client")
                                               ])
    def test_get_online_features(self, mocked_client, mocker):
        ROW_COUNT = 300

        mocked_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )

        fields = dict()
        for feature_num in range(1, 10):
            fields[f"my_project/feature_{str(feature_num)}:1"] = ValueProto.Value(
                int64_val=feature_num
            )
        field_values = GetOnlineFeaturesResponse.FieldValues(fields=fields)

        response = GetOnlineFeaturesResponse()
        entity_rows = []
        for row_number in range(1, ROW_COUNT + 1):
            response.field_values.append(field_values)
            entity_rows.append(
                GetOnlineFeaturesRequest.EntityRow(
                    fields={"customer_id": ValueProto.Value(int64_val=row_number)}
                )
            )

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetOnlineFeatures",
            return_value=response,
        )

        response = mocked_client.get_online_features(
            entity_rows=entity_rows,
            feature_refs=[
                "my_project/feature_1:1",
                "my_project/feature_2:1",
                "my_project/feature_3:1",
                "my_project/feature_4:1",
                "my_project/feature_5:1",
                "my_project/feature_6:1",
                "my_project/feature_7:1",
                "my_project/feature_8:1",
                "my_project/feature_9:1",
            ],
        )  # type: GetOnlineFeaturesResponse

        assert (
                response.field_values[0].fields["my_project/feature_1:1"].int64_val == 1
                and response.field_values[0].fields["my_project/feature_9:1"].int64_val == 9
        )

    @pytest.mark.parametrize("mocked_client", [pytest.lazy_fixture("mock_client"),
                                               pytest.lazy_fixture("secure_mock_client")
                                               ])
    def test_get_feature_set(self, mocked_client, mocker):
        mocked_client._core_service_stub = Core.CoreServiceStub(grpc.insecure_channel(""))

        from google.protobuf.duration_pb2 import Duration

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(
                feature_set=FeatureSetProto(
                    spec=FeatureSetSpecProto(
                        name="my_feature_set",
                        version=2,
                        max_age=Duration(seconds=3600),
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
        feature_set = mocked_client.get_feature_set("my_feature_set", version=2)

        assert (
                feature_set.name == "my_feature_set"
                and feature_set.version == 2
                and feature_set.fields["my_feature_1"].name == "my_feature_1"
                and feature_set.fields["my_feature_1"].dtype == ValueType.FLOAT
                and feature_set.fields["my_entity_1"].name == "my_entity_1"
                and feature_set.fields["my_entity_1"].dtype == ValueType.INT64
                and len(feature_set.features) == 2
                and len(feature_set.entities) == 1
        )

    @pytest.mark.parametrize("mocked_client", [pytest.lazy_fixture("mock_client"),
                                               pytest.lazy_fixture("secure_mock_client")
                                               ])
    def test_get_batch_features(self, mocked_client, mocker):

        mocked_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )
        mocked_client._core_service_stub = Core.CoreServiceStub(grpc.insecure_channel(""))

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(
                feature_set=FeatureSetProto(
                    spec=FeatureSetSpecProto(
                        name="customer_fs",
                        version=1,
                        project="my_project",
                        entities=[
                            EntitySpecProto(
                                name="customer", value_type=ValueProto.ValueType.INT64
                            ),
                            EntitySpecProto(
                                name="transaction",
                                value_type=ValueProto.ValueType.INT64,
                            ),
                        ],
                        features=[
                            FeatureSpecProto(
                                name="customer_feature_1",
                                value_type=ValueProto.ValueType.FLOAT,
                            ),
                            FeatureSpecProto(
                                name="customer_feature_2",
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
                "customer": [1001, 1002, 1003],
                "transaction": [1001, 1002, 1003],
                "my_project/customer_feature_1:1": [1001, 1002, 1003],
                "my_project/customer_feature_2:1": [1001, 1002, 1003],
            }
        )

        final_results = tempfile.mktemp()
        to_avro(file_path_or_buffer=final_results, df=expected_dataframe)

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetBatchFeatures",
            return_value=GetBatchFeaturesResponse(
                job=BatchFeaturesJob(
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
                job=BatchFeaturesJob(
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
        response = mocked_client.get_batch_features(
            entity_rows=pd.DataFrame(
                {
                    "datetime": [
                        pd.datetime.now(tz=timezone("Asia/Singapore")) for _ in range(3)
                    ],
                    "customer": [1001, 1002, 1003],
                    "transaction": [1001, 1002, 1003],
                }
            ),
            feature_refs=[
                "my_project/customer_feature_1:1",
                "my_project/customer_feature_2:1",
            ],
        )  # type: Job

        assert response.id == "123" and response.status == JobStatus.JOB_STATUS_DONE

        actual_dataframe = response.to_dataframe()

        assert actual_dataframe[
            ["my_project/customer_feature_1:1", "my_project/customer_feature_2:1"]
        ].equals(
            expected_dataframe[
                ["my_project/customer_feature_1:1", "my_project/customer_feature_2:1"]
            ]
        )

    @pytest.mark.parametrize("test_client", [pytest.lazy_fixture("client"),
                                             pytest.lazy_fixture("secure_client")
                                             ])
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
                and feature_sets[1].features[1].dtype == ValueType.BYTES_LIST
        )

    @pytest.mark.parametrize("dataframe,test_client", [(dataframes.GOOD, pytest.lazy_fixture("client")),
                                                       (dataframes.GOOD, pytest.lazy_fixture("secure_client"))])
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
        with patch("feast.client.get_producer") as mocked_queue:
            # Ingest data into Feast
            test_client.ingest("driver-feature-set", dataframe)

    @pytest.mark.parametrize("dataframe,exception,test_client",
                             [(dataframes.GOOD, TimeoutError, pytest.lazy_fixture("client")),
                              (dataframes.GOOD, TimeoutError, pytest.lazy_fixture("secure_client"))])
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
            with patch("feast.client.get_producer") as mocked_queue:
                # Ingest data into Feast
                test_client.ingest("driver-feature-set", dataframe, timeout=1)

    @pytest.mark.parametrize(
        "dataframe,exception,test_client",
        [
            (dataframes.BAD_NO_DATETIME, Exception, pytest.lazy_fixture("client")),
            (dataframes.BAD_INCORRECT_DATETIME_TYPE, Exception, pytest.lazy_fixture("client")),
            (dataframes.BAD_NO_ENTITY, Exception, pytest.lazy_fixture("client")),
            (dataframes.NO_FEATURES, Exception, pytest.lazy_fixture("client")),
            (dataframes.BAD_NO_DATETIME, Exception, pytest.lazy_fixture("secure_client")),
            (dataframes.BAD_INCORRECT_DATETIME_TYPE, Exception, pytest.lazy_fixture("secure_client")),
            (dataframes.BAD_NO_ENTITY, Exception, pytest.lazy_fixture("secure_client")),
            (dataframes.NO_FEATURES, Exception, pytest.lazy_fixture("secure_client")),
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

    @pytest.mark.parametrize("dataframe,test_client", [(dataframes.ALL_TYPES, pytest.lazy_fixture("client")),
                                                       (dataframes.ALL_TYPES, pytest.lazy_fixture("secure_client"))])
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
                #  type again https://github.com/gojek/feast/issues/341
                Feature(name="double_list_feature", dtype=ValueType.DOUBLE_LIST),
            ],
            max_age=Duration(seconds=3600),
        )

        # Register with Feast core
        test_client.apply(all_types_fs)

        mocker.patch.object(
            test_client._core_service_stub,
            "GetFeatureSet",
            return_value=GetFeatureSetResponse(feature_set=all_types_fs.to_proto()),
        )

        # Need to create a mock producer
        with patch("feast.client.get_producer") as mocked_queue:
            # Ingest data into Feast
            test_client.ingest(all_types_fs, dataframe)

    @patch("grpc.channel_ready_future")
    def test_secure_channel_creation_with_secure_client(self, _mocked_obj):
        client = Client(core_url="localhost:50051", serving_url="localhost:50052", serving_secure=True,
                        core_secure=True)
        with mock.patch("grpc.secure_channel") as _grpc_mock, \
                mock.patch("grpc.ssl_channel_credentials", MagicMock(return_value="test")) as _mocked_credentials:
            client._connect_serving()
            _grpc_mock.assert_called_with(client.serving_url, _mocked_credentials.return_value)

    @mock.patch("grpc.channel_ready_future")
    def test_secure_channel_creation_with_secure_serving_url(self, _mocked_obj, ):
        client = Client(core_url="localhost:50051", serving_url="localhost:443")
        with mock.patch("grpc.secure_channel") as _grpc_mock, \
                mock.patch("grpc.ssl_channel_credentials", MagicMock(return_value="test")) as _mocked_credentials:
            client._connect_serving()
            _grpc_mock.assert_called_with(client.serving_url, _mocked_credentials.return_value)

    @patch("grpc.channel_ready_future")
    def test_secure_channel_creation_with_secure_client(self, _mocked_obj):
        client = Client(core_url="localhost:50053", serving_url="localhost:50054", serving_secure=True,
                        core_secure=True)
        with mock.patch("grpc.secure_channel") as _grpc_mock, \
                mock.patch("grpc.ssl_channel_credentials", MagicMock(return_value="test")) as _mocked_credentials:
            client._connect_core()
            _grpc_mock.assert_called_with(client.core_url, _mocked_credentials.return_value)

    @patch("grpc.channel_ready_future")
    def test_secure_channel_creation_with_secure_core_url(self, _mocked_obj):
        client = Client(core_url="localhost:443", serving_url="localhost:50054")
        with mock.patch("grpc.secure_channel") as _grpc_mock, \
                mock.patch("grpc.ssl_channel_credentials", MagicMock(return_value="test")) as _mocked_credentials:
            client._connect_core()
            _grpc_mock.assert_called_with(client.core_url, _mocked_credentials.return_value)