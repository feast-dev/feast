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

import grpc
import pandas as pd
import numpy as np
import feast.core.CoreService_pb2_grpc as Core
import feast.serving.ServingService_pb2_grpc as Serving
from feast.core.FeatureSet_pb2 import FeatureSetSpec, FeatureSpec, EntitySpec
from feast.core.Source_pb2 import SourceType, KafkaSourceConfig, Source
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionResponse,
    GetFeatureSetsResponse,
)
from feast.serving.ServingService_pb2 import (
    GetFeastServingInfoResponse,
    GetOnlineFeaturesResponse,
    GetOnlineFeaturesRequest,
)
from google.protobuf.timestamp_pb2 import Timestamp
import pytest
from feast.client import Client
from concurrent import futures
from feast_core_server import CoreServicer
from feast_serving_server import ServingServicer
from feast.types import FeatureRow_pb2 as FeatureRowProto
from feast.types import Field_pb2 as FieldProto
from feast.types import Value_pb2 as ValueProto
from feast.value_type import ValueType


CORE_URL = "core.feast.example.com"
SERVING_URL = "serving.example.com"


class TestClient:
    @pytest.fixture(scope="function")
    def core_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        server.add_insecure_port("[::]:50051")
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture(scope="function")
    def serving_server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Serving.add_ServingServiceServicer_to_server(ServingServicer(), server)
        server.add_insecure_port("[::]:50052")
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def mock_client(self, mocker):
        client = Client(core_url=CORE_URL, serving_url=SERVING_URL)
        mocker.patch.object(client, "_connect_core")
        mocker.patch.object(client, "_connect_serving")
        return client

    @pytest.fixture
    def client_with_servers(self, core_server, serving_server):
        return Client(core_url="localhost:50051", serving_url="localhost:50052")

    def test_version(self, mock_client, mocker):
        mock_client._core_service_stub = Core.CoreServiceStub(grpc.insecure_channel(""))
        mock_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )

        mocker.patch.object(
            mock_client._core_service_stub,
            "GetFeastCoreVersion",
            return_value=GetFeastCoreVersionResponse(version="0.3.0"),
        )

        mocker.patch.object(
            mock_client._serving_service_stub,
            "GetFeastServingInfo",
            return_value=GetFeastServingInfoResponse(version="0.3.0"),
        )

        status = mock_client.version()
        assert (
            status["core"]["url"] == CORE_URL
            and status["core"]["version"] == "0.3.0"
            and status["serving"]["url"] == SERVING_URL
            and status["serving"]["version"] == "0.3.0"
        )

    def test_get_feature(self, mock_client, mocker):
        ROW_COUNT = 300

        mock_client._serving_service_stub = Serving.ServingServiceStub(
            grpc.insecure_channel("")
        )

        fields = dict()
        for feature_num in range(1, 10):
            fields["feature_set_1:1:feature_" + str(feature_num)] = ValueProto.Value(
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
            mock_client._serving_service_stub,
            "GetOnlineFeatures",
            return_value=response,
        )

        response = mock_client.get_online_features(
            entity_rows=entity_rows,
            feature_ids=[
                "feature_set_1:1:feature_1",
                "feature_set_1:1:feature_2",
                "feature_set_1:1:feature_3",
                "feature_set_1:1:feature_4",
                "feature_set_1:1:feature_5",
                "feature_set_1:1:feature_6",
                "feature_set_1:1:feature_7",
                "feature_set_1:1:feature_8",
                "feature_set_1:1:feature_9",
            ],
        )  # type: GetOnlineFeaturesResponse

        assert (
            response.field_values[0].fields["feature_set_1:1:feature_1"].int64_val == 1
            and response.field_values[0].fields["feature_set_1:1:feature_9"].int64_val
            == 9
        )

    def test_get_feature_set(self, mock_client, mocker):
        mock_client._core_service_stub = Core.CoreServiceStub(grpc.insecure_channel(""))

        from google.protobuf.duration_pb2 import Duration

        mocker.patch.object(
            mock_client._core_service_stub,
            "GetFeatureSets",
            return_value=GetFeatureSetsResponse(
                feature_sets=[
                    FeatureSetSpec(
                        name="my_feature_set",
                        version=2,
                        max_age=Duration(seconds=3600),
                        features=[
                            FeatureSpec(
                                name="my_feature_1",
                                value_type=ValueProto.ValueType.FLOAT,
                            ),
                            FeatureSpec(
                                name="my_feature_2",
                                value_type=ValueProto.ValueType.FLOAT,
                            ),
                        ],
                        entities=[
                            EntitySpec(
                                name="my_entity_1",
                                value_type=ValueProto.ValueType.INT64,
                            )
                        ],
                        source=Source(
                            type=SourceType.KAFKA,
                            kafka_source_config=KafkaSourceConfig(),
                        ),
                    )
                ]
            ),
        )

        feature_set = mock_client.get_feature_set("my_feature_set", version=2)

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
