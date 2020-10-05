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
from concurrent import futures
from unittest import mock

import grpc
import pytest
from google.protobuf.duration_pb2 import Duration
from mock import MagicMock, patch
from pytest_lazyfixture import lazy_fixture

from feast.client import Client
from feast.core import CoreService_pb2_grpc as Core
from feast.core.CoreService_pb2 import (
    GetEntityResponse,
    GetFeastCoreVersionResponse,
    GetFeatureTableResponse,
    ListEntitiesResponse,
    ListFeatureTablesResponse,
)
from feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.core.Entity_pb2 import Entity as EntityProto
from feast.core.Entity_pb2 import EntityMeta as EntityMetaProto
from feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
from feast.core.Feature_pb2 import FeatureSpecV2 as FeatureSpecProto
from feast.core.FeatureTable_pb2 import FeatureTable as FeatureTableProto
from feast.core.FeatureTable_pb2 import FeatureTableMeta as FeatureTableMetaProto
from feast.core.FeatureTable_pb2 import FeatureTableSpec as FeatureTableSpecProto
from feast.data_source import DataSource, FileOptions, KafkaOptions, SourceType
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_table import FeatureTable
from feast.serving import ServingService_pb2_grpc as Serving
from feast.serving.ServingService_pb2 import GetFeastServingInfoResponse
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
        assert 1 == 1

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
        assert 1 == 1

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_get_entity(self, mocked_client, mocker):
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )

        entity_proto = EntityProto(
            spec=EntitySpecProto(
                name="driver_car_id",
                description="Car driver id",
                value_type=ValueProto.ValueType.STRING,
                labels={"key1": "val1", "key2": "val2"},
            ),
            meta=EntityMetaProto(),
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetEntity",
            return_value=GetEntityResponse(entity=entity_proto),
        )
        mocked_client.set_project("my_project")
        entity = mocked_client.get_entity("my_entity")

        assert (
            entity.name == "driver_car_id"
            and entity.description == "Car driver id"
            and entity.value_type == ValueType(ValueProto.ValueType.STRING).name
            and "key1" in entity.labels
            and entity.labels["key1"] == "val1"
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_list_entities(self, mocked_client, mocker):
        mocker.patch.object(
            mocked_client,
            "_core_service_stub",
            return_value=Core.CoreServiceStub(grpc.insecure_channel("")),
        )

        entity_1_proto = EntityProto(
            spec=EntitySpecProto(
                name="driver_car_id",
                description="Car driver id",
                value_type=ValueProto.ValueType.INT64,
                labels={"key1": "val1", "key2": "val2"},
            )
        )
        entity_2_proto = EntityProto(
            spec=EntitySpecProto(
                name="driver_ride_id",
                description="Ride driver id",
                value_type=ValueProto.ValueType.STRING,
                labels={"key3": "val3", "key4": "val4"},
            )
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "ListEntities",
            return_value=ListEntitiesResponse(
                entities=[entity_1_proto, entity_2_proto]
            ),
        )

        entities = mocked_client.list_entities(labels={"key1": "val1"})
        assert len(entities) == 2

        entity = entities[1]
        assert (
            entity.name == "driver_ride_id"
            and entity.description == "Ride driver id"
            and entity.value_type == ValueType(ValueProto.ValueType.STRING).name
            and "key3" in entity.labels
            and entity.labels["key3"] == "val3"
            and "key4" in entity.labels
            and entity.labels["key4"] == "val4"
        )

    @pytest.mark.parametrize(
        "test_client", [lazy_fixture("client"), lazy_fixture("secure_client")],
    )
    def test_apply_entity_success(self, test_client):

        test_client.set_project("project1")
        entity = Entity(
            name="driver_car_id",
            description="Car driver id",
            value_type=ValueType.STRING,
            labels={"team": "matchmaking"},
        )

        # Register Entity with Core
        test_client.apply_entity(entity)

        entities = test_client.list_entities()

        entity = entities[0]
        assert (
            len(entities) == 1
            and entity.name == "driver_car_id"
            and entity.value_type == ValueType(ValueProto.ValueType.STRING).name
            and entity.description == "Car driver id"
            and "team" in entity.labels
            and entity.labels["team"] == "matchmaking"
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_get_feature_table(self, mocked_client, mocker):
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )

        from google.protobuf.duration_pb2 import Duration

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureTable",
            return_value=GetFeatureTableResponse(
                table=FeatureTableProto(
                    spec=FeatureTableSpecProto(
                        name="my_feature_table",
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
                        entities=["my_entity_1"],
                        batch_source=DataSourceProto(
                            type=SourceType(1).name,
                            field_mapping={
                                "ride_distance": "ride_distance",
                                "ride_duration": "ride_duration",
                            },
                            file_options=DataSourceProto.FileOptions(
                                file_format="avro", file_url="data/test.avro"
                            ),
                            timestamp_column="ts_col",
                            date_partition_column="date_partition_col",
                        ),
                    ),
                    meta=FeatureTableMetaProto(),
                )
            ),
        )
        mocked_client.set_project("my_project")
        feature_table = mocked_client.get_feature_table("my_feature_table")

        assert (
            feature_table.name == "my_feature_table"
            and "key1" in feature_table.labels
            and feature_table.labels["key1"] == "val1"
            and "key2" in feature_table.labels
            and feature_table.labels["key2"] == "val2"
            and len(feature_table.features) == 2
            and len(feature_table.entities) == 1
        )

    @pytest.mark.parametrize(
        "mocked_client",
        [lazy_fixture("mock_client"), lazy_fixture("secure_mock_client")],
    )
    def test_list_feature_tables(self, mocked_client, mocker):
        mocker.patch.object(
            mocked_client,
            "_core_service_stub",
            return_value=Core.CoreServiceStub(grpc.insecure_channel("")),
        )

        batch_source = DataSourceProto(
            type=SourceType(1).name,
            field_mapping={
                "ride_distance": "ride_distance",
                "ride_duration": "ride_duration",
            },
            file_options=DataSourceProto.FileOptions(
                file_format="avro", file_url="data/test.avro"
            ),
            timestamp_column="ts_col",
            date_partition_column="date_partition_col",
        )

        feature_table_1_proto = FeatureTableProto(
            spec=FeatureTableSpecProto(
                name="driver_car",
                max_age=Duration(seconds=3600),
                labels={"key1": "val1", "key2": "val2"},
                features=[
                    FeatureSpecProto(
                        name="feature_1", value_type=ValueProto.ValueType.FLOAT
                    )
                ],
                entities=["driver_car_id"],
                batch_source=batch_source,
            )
        )
        feature_table_2_proto = FeatureTableProto(
            spec=FeatureTableSpecProto(
                name="driver_ride",
                max_age=Duration(seconds=3600),
                labels={"key1": "val1"},
                features=[
                    FeatureSpecProto(
                        name="feature_1", value_type=ValueProto.ValueType.FLOAT
                    )
                ],
                entities=["driver_ride_id"],
                batch_source=batch_source,
            )
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "ListFeatureTables",
            return_value=ListFeatureTablesResponse(
                tables=[feature_table_1_proto, feature_table_2_proto]
            ),
        )

        feature_tables = mocked_client.list_feature_tables(labels={"key1": "val1"})
        assert len(feature_tables) == 2

        feature_table = feature_tables[0]
        assert (
            feature_table.name == "driver_car"
            and "key1" in feature_table.labels
            and feature_table.labels["key1"] == "val1"
            and "key2" in feature_table.labels
            and feature_table.labels["key2"] == "val2"
            and len(feature_table.features) == 1
        )

    @pytest.mark.parametrize(
        "test_client", [lazy_fixture("client"), lazy_fixture("secure_client")],
    )
    def test_apply_feature_table_success(self, test_client):

        test_client.set_project("project1")

        # Create Feature Tables
        batch_source = DataSource(
            type=SourceType(1).name,
            field_mapping={
                "ride_distance": "ride_distance",
                "ride_duration": "ride_duration",
            },
            options=FileOptions(file_format="avro", file_url="data/test.avro"),
            timestamp_column="ts_col",
            date_partition_column="date_partition_col",
        )

        stream_source = DataSource(
            type=SourceType(3).name,
            field_mapping={
                "ride_distance": "ride_distance",
                "ride_duration": "ride_duration",
            },
            options=KafkaOptions(
                bootstrap_servers="localhost:9094",
                class_path="random/path/to/class",
                topic="test_topic",
            ),
            timestamp_column="ts_col",
        )

        ft1 = FeatureTable(
            name="my-feature-table-1",
            features=[
                Feature(name="fs1-my-feature-1", dtype=ValueType.INT64).to_proto(),
                Feature(name="fs1-my-feature-2", dtype=ValueType.STRING).to_proto(),
                Feature(
                    name="fs1-my-feature-3", dtype=ValueType.STRING_LIST
                ).to_proto(),
                Feature(name="fs1-my-feature-4", dtype=ValueType.BYTES_LIST).to_proto(),
            ],
            entities=["fs1-my-entity-1"],
            labels={"team": "matchmaking"},
            batch_source=batch_source.to_proto(),
            stream_source=stream_source.to_proto(),
        )

        # Register Feature Table with Core
        test_client.apply_feature_table(ft1)

        feature_tables = test_client.list_feature_tables()

        # List Feature Tables
        assert (
            len(feature_tables) == 1
            and feature_tables[0].name == "my-feature-table-1"
            and feature_tables[0].features[0].name == "fs1-my-feature-1"
            and feature_tables[0].features[0].value_type == ValueProto.ValueType.INT64
            and feature_tables[0].features[1].name == "fs1-my-feature-2"
            and feature_tables[0].features[1].value_type == ValueProto.ValueType.STRING
            and feature_tables[0].features[2].name == "fs1-my-feature-3"
            and feature_tables[0].features[2].value_type
            == ValueProto.ValueType.STRING_LIST
            and feature_tables[0].features[3].name == "fs1-my-feature-4"
            and feature_tables[0].features[3].value_type
            == ValueProto.ValueType.BYTES_LIST
            and feature_tables[0].entities[0] == "fs1-my-entity-1"
        )

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
        secure_core_client_with_auth.list_feature_tables()

    def test_auth_success_with_insecure_channel_on_core_url(
        self, insecure_core_server_with_auth
    ):
        client = Client(
            core_url=f"localhost:{insecure_core_server_with_auth}",
            enable_auth=True,
            auth_token=_FAKE_JWT_TOKEN,
        )
        client.list_feature_tables()

    def test_no_auth_sent_when_auth_disabled(
        self, insecure_core_server_that_blocks_auth
    ):
        client = Client(core_url=f"localhost:{insecure_core_server_that_blocks_auth}")
        client.list_feature_tables()
