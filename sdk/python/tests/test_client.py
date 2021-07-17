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
import os
import pkgutil
import socket
from concurrent import futures
from datetime import datetime, timedelta
from typing import Tuple
from unittest import mock

import grpc
import numpy as np
import pandas as pd
import pytest
import pytz
from google.protobuf.duration_pb2 import Duration
from mock import MagicMock, patch
from pandas.util.testing import assert_frame_equal
from pyarrow import parquet as pq
from pytest_lazyfixture import lazy_fixture

from feast import FileSource
from feast.client import Client
from feast.data_format import ParquetFormat, ProtoFormat
from feast.data_source import KafkaSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_table import FeatureTable
from feast.protos.feast.core import CoreService_pb2_grpc as Core
from feast.protos.feast.core.CoreService_pb2 import (
    GetFeastCoreVersionResponse,
    GetFeatureTableResponse,
    ListFeaturesResponse,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2 as FeatureSpecProto
from feast.protos.feast.core.FeatureTable_pb2 import FeatureTable as FeatureTableProto
from feast.protos.feast.core.FeatureTable_pb2 import (
    FeatureTableMeta as FeatureTableMetaProto,
)
from feast.protos.feast.core.FeatureTable_pb2 import (
    FeatureTableSpec as FeatureTableSpecProto,
)
from feast.protos.feast.serving import ServingService_pb2_grpc as Serving
from feast.protos.feast.serving.ServingService_pb2 import (
    FeatureReferenceV2 as FeatureRefProto,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    GetFeastServingInfoResponse,
    GetOnlineFeaturesRequestV2,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types import Value_pb2 as ValueProto
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

    @pytest.fixture
    def partitioned_df(self):
        # Partitioned DataFrame
        N_ROWS = 100
        time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
        final_offset = (
            [time_offset] * 33
            + [time_offset - timedelta(days=1)] * 33
            + [time_offset - timedelta(days=2)] * 34
        )
        final_part_offset = (
            [time_offset - timedelta(days=99)] * 33
            + [time_offset - timedelta(days=100)] * 33
            + [time_offset - timedelta(days=101)] * 34
        )
        return pd.DataFrame(
            {
                "datetime": final_offset,
                "datetime_col": final_part_offset,
                "dev_feature_float": [np.float(row) for row in range(N_ROWS)],
                "dev_feature_string": ["feat_" + str(row) for row in range(N_ROWS)],
            }
        )

    @pytest.fixture
    def non_partitioned_df(self):
        # Non-Partitioned DataFrame
        N_ROWS = 100
        time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
        return pd.DataFrame(
            {
                "datetime": [time_offset] * N_ROWS,
                "dev_feature_float": [np.float(row) for row in range(N_ROWS)],
                "dev_feature_string": ["feat_" + str(row) for row in range(N_ROWS)],
            }
        )

    @pytest.fixture
    def get_online_features_fields_statuses(self):
        ROW_COUNT = 100
        fields_statuses_tuple_list = []
        for row_number in range(0, ROW_COUNT):
            fields_statuses_tuple_list.append(
                (
                    {
                        "driver_id": ValueProto.Value(int64_val=row_number),
                        "driver:age": ValueProto.Value(int64_val=1),
                        "driver:rating": ValueProto.Value(string_val="9"),
                        "driver:null_value": ValueProto.Value(),
                    },
                    {
                        "driver_id": GetOnlineFeaturesResponse.FieldStatus.PRESENT,
                        "driver:age": GetOnlineFeaturesResponse.FieldStatus.PRESENT,
                        "driver:rating": GetOnlineFeaturesResponse.FieldStatus.PRESENT,
                        "driver:null_value": GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE,
                    },
                )
            )
        return fields_statuses_tuple_list

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
        "test_client", [lazy_fixture("client"), lazy_fixture("secure_client")],
    )
    def test_apply_entity_success(self, test_client):

        entity = Entity(
            name="driver_car_id",
            description="Car driver id",
            value_type=ValueType.STRING,
            labels={"team": "matchmaking"},
        )

        # Register Entity with Core
        test_client.apply(entity)

        entities = test_client.list_entities()

        entity = entities[0]
        assert (
            len(entities) == 1
            and entity.name == "driver_car_id"
            and entity.value_type == ValueType(ValueProto.ValueType.STRING)
            and entity.description == "Car driver id"
            and "team" in entity.labels
            and entity.labels["team"] == "matchmaking"
        )

    @pytest.mark.parametrize(
        "test_client", [lazy_fixture("client"), lazy_fixture("secure_client")],
    )
    def test_apply_feature_table_success(self, test_client):

        # Create Feature Tables
        batch_source = FileSource(
            file_format=ParquetFormat(),
            file_url="file://feast/*",
            event_timestamp_column="ts_col",
            created_timestamp_column="timestamp",
            date_partition_column="date_partition_col",
        )

        stream_source = KafkaSource(
            bootstrap_servers="localhost:9094",
            message_format=ProtoFormat("class.path"),
            topic="test_topic",
            event_timestamp_column="ts_col",
        )

        ft1 = FeatureTable(
            name="my-feature-table-1",
            features=[
                Feature(name="fs1-my-feature-1", dtype=ValueType.INT64),
                Feature(name="fs1-my-feature-2", dtype=ValueType.STRING),
                Feature(name="fs1-my-feature-3", dtype=ValueType.STRING_LIST),
                Feature(name="fs1-my-feature-4", dtype=ValueType.BYTES_LIST),
            ],
            entities=["fs1-my-entity-1"],
            labels={"team": "matchmaking"},
            batch_source=batch_source,
            stream_source=stream_source,
        )

        # Register Feature Table with Core
        test_client.apply(ft1)

        feature_tables = test_client.list_feature_tables()

        # List Feature Tables
        assert (
            len(feature_tables) == 1
            and feature_tables[0].name == "my-feature-table-1"
            and feature_tables[0].features[0].name == "fs1-my-feature-1"
            and feature_tables[0].features[0].dtype == ValueType.INT64
            and feature_tables[0].features[1].name == "fs1-my-feature-2"
            and feature_tables[0].features[1].dtype == ValueType.STRING
            and feature_tables[0].features[2].name == "fs1-my-feature-3"
            and feature_tables[0].features[2].dtype == ValueType.STRING_LIST
            and feature_tables[0].features[3].name == "fs1-my-feature-4"
            and feature_tables[0].features[3].dtype == ValueType.BYTES_LIST
            and feature_tables[0].entities[0] == "fs1-my-entity-1"
        )

    @pytest.mark.parametrize(
        "test_client", [lazy_fixture("client"), lazy_fixture("secure_client")]
    )
    def test_list_features(self, test_client, mocker):
        mocker.patch.object(
            test_client,
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
            test_client._core_service_stub,
            "ListFeatures",
            return_value=ListFeaturesResponse(
                features={
                    "driver_car:feature_1": feature1_proto,
                    "driver_car:feature_2": feature2_proto,
                }
            ),
        )

        features = test_client.list_features_by_ref(project="test")
        assert len(features) == 2

        native_feature_list = []
        for _, feature_proto in features.items():
            native_feature_list.append(feature_proto)

        assert sorted(native_feature_list) == sorted(
            [Feature.from_proto(feature1_proto), Feature.from_proto(feature2_proto)]
        )

    @pytest.mark.parametrize(
        "mocked_client", [lazy_fixture("mock_client")],
    )
    def test_ingest_dataframe_partition(
        self, mocked_client, mocker, partitioned_df, tmp_path
    ):
        """
        Test ingestion with local FileSource, using DataFrame.
        Partition column stated but not provided in Dataset.
        """
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureTable",
            return_value=_ingest_test_getfeaturetable_mocked_resp(
                f"file://{tmp_path}", "date"
            ),
        )

        mocked_client.set_project("my_project")
        ft = mocked_client.get_feature_table("ingest_featuretable")
        mocked_client.ingest(ft, partitioned_df, timeout=600)

        pq_df = pq.read_table(tmp_path).to_pandas().drop(columns=["date"])

        partitioned_df, pq_df = _ingest_test_format_dataframes(
            partitioned_df, pq_df, True
        )

        assert_frame_equal(partitioned_df, pq_df)

    @pytest.mark.parametrize(
        "mocked_client", [lazy_fixture("mock_client")],
    )
    def test_ingest_dataframe_no_partition(
        self, mocked_client, mocker, non_partitioned_df, tmp_path
    ):
        """
        Test ingestion with local FileSource, using DataFrame.
        Partition column not stated.
        """
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureTable",
            return_value=_ingest_test_getfeaturetable_mocked_resp(f"file://{tmp_path}"),
        )

        mocked_client.set_project("my_project")
        ft = mocked_client.get_feature_table("ingest_featuretable")
        mocked_client.ingest(ft, non_partitioned_df, timeout=600)

        # Since not partitioning, we're only looking for single file
        single_file = [
            f for f in os.listdir(tmp_path) if os.path.isfile(os.path.join(tmp_path, f))
        ][0]
        pq_df = pq.read_table(tmp_path / single_file).to_pandas()

        non_partitioned_df, pq_df = _ingest_test_format_dataframes(
            non_partitioned_df, pq_df
        )

        assert_frame_equal(non_partitioned_df, pq_df)

    @pytest.mark.parametrize(
        "mocked_client", [lazy_fixture("mock_client")],
    )
    def test_ingest_csv(self, mocked_client, mocker, tmp_path):
        """
        Test ingestion with local FileSource, using CSV file.
        Partition column is provided.
        """
        mocked_client._core_service_stub = Core.CoreServiceStub(
            grpc.insecure_channel("")
        )

        mocker.patch.object(
            mocked_client._core_service_stub,
            "GetFeatureTable",
            return_value=_ingest_test_getfeaturetable_mocked_resp(
                f"file://{tmp_path}", "date"
            ),
        )

        partitioned_df = pd.read_csv(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "./data/dev_featuretable.csv",
            ),
            parse_dates=["datetime"],
        )

        mocked_client.set_project("my_project")
        ft = mocked_client.get_feature_table("ingest_featuretable")
        mocked_client.ingest(ft, partitioned_df, timeout=600)

        pq_df = pq.read_table(tmp_path).to_pandas().drop(columns=["date"])

        partitioned_df, pq_df = _ingest_test_format_dataframes(
            partitioned_df, pq_df, True
        )

        assert_frame_equal(partitioned_df, pq_df)

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
    def test_get_online_features(
        self, mocked_client, auth_metadata, mocker, get_online_features_fields_statuses
    ):
        ROW_COUNT = 100

        mocked_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )

        request = GetOnlineFeaturesRequestV2(project="driver_project")
        request.features.extend(
            [
                FeatureRefProto(feature_table="driver", name="age"),
                FeatureRefProto(feature_table="driver", name="rating"),
                FeatureRefProto(feature_table="driver", name="null_value"),
            ]
        )

        receive_response = GetOnlineFeaturesResponse()
        entity_rows = []
        for row_number in range(0, ROW_COUNT):
            fields = get_online_features_fields_statuses[row_number][0]
            statuses = get_online_features_fields_statuses[row_number][1]
            request.entity_rows.append(
                GetOnlineFeaturesRequestV2.EntityRow(
                    fields={"driver_id": ValueProto.Value(int64_val=row_number)}
                )
            )
            entity_rows.append({"driver_id": ValueProto.Value(int64_val=row_number)})
            receive_response.field_values.append(
                GetOnlineFeaturesResponse.FieldValues(fields=fields, statuses=statuses)
            )

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetOnlineFeaturesV2",
            return_value=receive_response,
        )
        got_response = mocked_client.get_online_features(
            entity_rows=entity_rows,
            feature_refs=["driver:age", "driver:rating", "driver:null_value"],
            project="driver_project",
        )
        mocked_client._serving_service_stub.GetOnlineFeaturesV2.assert_called_with(
            request, metadata=auth_metadata, timeout=10
        )

        got_fields = got_response.field_values[1].fields
        got_statuses = got_response.field_values[1].statuses
        assert (
            got_fields["driver_id"] == ValueProto.Value(int64_val=1)
            and got_statuses["driver_id"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver:age"] == ValueProto.Value(int64_val=1)
            and got_statuses["driver:age"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver:rating"] == ValueProto.Value(string_val="9")
            and got_statuses["driver:rating"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver:null_value"] == ValueProto.Value()
            and got_statuses["driver:null_value"]
            == GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE
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
    def test_get_online_features_multi_entities(
        self, mocked_client, auth_metadata, mocker, get_online_features_fields_statuses
    ):
        ROW_COUNT = 100

        mocked_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )

        request = GetOnlineFeaturesRequestV2(project="driver_project")
        request.features.extend(
            [
                FeatureRefProto(feature_table="driver", name="age"),
                FeatureRefProto(feature_table="driver", name="rating"),
                FeatureRefProto(feature_table="driver", name="null_value"),
            ]
        )

        receive_response = GetOnlineFeaturesResponse()
        entity_rows = []
        for row_number in range(0, ROW_COUNT):
            fields = get_online_features_fields_statuses[row_number][0]
            fields["driver_id2"] = ValueProto.Value(int64_val=1)
            statuses = get_online_features_fields_statuses[row_number][1]
            statuses["driver_id2"] = GetOnlineFeaturesResponse.FieldStatus.PRESENT

            request.entity_rows.append(
                GetOnlineFeaturesRequestV2.EntityRow(
                    fields={
                        "driver_id": ValueProto.Value(int64_val=row_number),
                        "driver_id2": ValueProto.Value(int64_val=row_number),
                    }
                )
            )
            entity_rows.append(
                {
                    "driver_id": ValueProto.Value(int64_val=row_number),
                    "driver_id2": ValueProto.Value(int64_val=row_number),
                }
            )
            receive_response.field_values.append(
                GetOnlineFeaturesResponse.FieldValues(fields=fields, statuses=statuses)
            )

        mocker.patch.object(
            mocked_client._serving_service_stub,
            "GetOnlineFeaturesV2",
            return_value=receive_response,
        )
        got_response = mocked_client.get_online_features(
            entity_rows=entity_rows,
            feature_refs=["driver:age", "driver:rating", "driver:null_value"],
            project="driver_project",
        )
        mocked_client._serving_service_stub.GetOnlineFeaturesV2.assert_called_with(
            request, metadata=auth_metadata, timeout=10
        )

        got_fields = got_response.field_values[1].fields
        got_statuses = got_response.field_values[1].statuses
        assert (
            got_fields["driver_id"] == ValueProto.Value(int64_val=1)
            and got_statuses["driver_id"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver_id2"] == ValueProto.Value(int64_val=1)
            and got_statuses["driver_id2"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver:age"] == ValueProto.Value(int64_val=1)
            and got_statuses["driver:age"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver:rating"] == ValueProto.Value(string_val="9")
            and got_statuses["driver:rating"]
            == GetOnlineFeaturesResponse.FieldStatus.PRESENT
            and got_fields["driver:null_value"] == ValueProto.Value()
            and got_statuses["driver:null_value"]
            == GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE
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


def _ingest_test_getfeaturetable_mocked_resp(
    file_url: str, date_partition_col: str = ""
):
    return GetFeatureTableResponse(
        table=FeatureTableProto(
            spec=FeatureTableSpecProto(
                name="ingest_featuretable",
                max_age=Duration(seconds=3600),
                features=[
                    FeatureSpecProto(
                        name="dev_feature_float", value_type=ValueProto.ValueType.FLOAT,
                    ),
                    FeatureSpecProto(
                        name="dev_feature_string",
                        value_type=ValueProto.ValueType.STRING,
                    ),
                ],
                entities=["dev_entity"],
                batch_source=DataSourceProto(
                    file_options=DataSourceProto.FileOptions(
                        file_format=ParquetFormat().to_proto(), file_url=file_url
                    ),
                    event_timestamp_column="datetime",
                    created_timestamp_column="timestamp",
                    date_partition_column=date_partition_col,
                ),
            ),
            meta=FeatureTableMetaProto(),
        )
    )


def _ingest_test_format_dataframes(
    partitioned_df: pd.DataFrame, pq_df: pd.DataFrame, with_partitions: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Format Dataframes before comparing them through assertion.

    Args:
        partitioned_df: DataFrame from pytest fixture
        pq_df: DataFrame from parquet files
        with_partitions: Flag to indicate if data has been partitioned

    Returns:
        Formatted DataFrames for comparison
    """
    partitioned_df.sort_values(by=["dev_feature_float"], inplace=True)
    pq_df.sort_values(by=["dev_feature_float"], inplace=True)
    pq_df = pq_df.reindex(sorted(pq_df.columns), axis=1)
    partitioned_df = partitioned_df.reindex(sorted(partitioned_df.columns), axis=1)
    partitioned_df.reset_index(drop=True, inplace=True)
    pq_df.reset_index(drop=True, inplace=True)

    if with_partitions:
        partitioned_df["datetime_col"] = pd.to_datetime(
            partitioned_df.datetime_col
        ).dt.tz_convert("UTC")
        pq_df["datetime_col"] = pd.to_datetime(pq_df.datetime_col).dt.tz_convert("UTC")

    return partitioned_df, pq_df
