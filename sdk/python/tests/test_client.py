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
from feast.core.CoreService_pb2 import GetFeastCoreVersionResponse
from feast.serving.ServingService_pb2 import (
    GetFeastServingVersionResponse,
    GetOnlineFeaturesResponse,
)
from google.protobuf.timestamp_pb2 import Timestamp
import pytest
from feast.client import Client
from concurrent import futures
from tests.feast_core_server import CoreServicer
from tests.feast_serving_server import ServingServicer
from feast.types import FeatureRow_pb2 as FeatureRowProto
from feast.types import Field_pb2 as FieldProto
from feast.types import Value_pb2 as ValueProto

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
            "GetFeastServingVersion",
            return_value=GetFeastServingVersionResponse(version="0.3.0"),
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

        response = GetOnlineFeaturesResponse()

        fields = dict()
        for feature_num in range(1, 10):
            fields["feature_set_1:1.feature_" + str(feature_num)] = ValueProto.Value(
                int64_val=feature_num
            )

        field_values = GetOnlineFeaturesResponse.FieldValues(fields=fields)

        for row_number in range(1, ROW_COUNT + 1):
            response.field_values.append(field_values)

        mocker.patch.object(
            mock_client._serving_service_stub,
            "GetOnlineFeatures",
            return_value=response,
        )

        entity_data = pd.DataFrame(
            {"datetime": np.repeat(4, ROW_COUNT), "entity_id": np.repeat(4, ROW_COUNT)}
        )

        feature_dataframe = mock_client.get(
            entity_data=entity_data,
            feature_ids=[
                "feature_set_1:1.feature_1",
                "feature_set_1:1.feature_2",
                "feature_set_1:1.feature_3",
                "feature_set_1:1.feature_4",
                "feature_set_1:1.feature_5",
                "feature_set_1:1.feature_6",
                "feature_set_1:1.feature_7",
                "feature_set_1:1.feature_8",
                "feature_set_1:1.feature_9",
            ],
        )

        feature_dataframe.head()
