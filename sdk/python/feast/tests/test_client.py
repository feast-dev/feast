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
import feast.core.CoreService_pb2_grpc as Core
import feast.serving.ServingService_pb2_grpc as Serving
from feast.core.CoreService_pb2 import GetFeastCoreVersionResponse
from feast.serving.ServingService_pb2 import GetFeastServingVersionResponse
import pytest
from feast.client import Client

CORE_URL = "core.feast.ai"
SERVING_URL = "serving.feast.ai"


class TestClient:
    @pytest.fixture
    def client(self, mocker):
        client = Client(core_url=CORE_URL, serving_url=SERVING_URL)
        mocker.patch.object(client, "_connect_core")
        mocker.patch.object(client, "_connect_serving")
        return client

    def test_version(self, client: Client, mocker):
        core_grpc_stub = Core.CoreServiceStub(grpc.insecure_channel(""))
        serving_grpc_stub = Serving.ServingServiceStub(grpc.insecure_channel(""))

        mocker.patch.object(
            core_grpc_stub,
            "GetFeastCoreVersion",
            return_value=GetFeastCoreVersionResponse(version="0.3.0"),
        )

        mocker.patch.object(
            serving_grpc_stub,
            "GetFeastServingVersion",
            return_value=GetFeastServingVersionResponse(version="0.3.0"),
        )

        client._core_service_stub = core_grpc_stub
        client._serving_service_stub = serving_grpc_stub

        status = client.version()
        assert (
            status["core"]["url"] == CORE_URL
            and status["core"]["version"] == "0.3.0"
            and status["serving"]["url"] == SERVING_URL
            and status["serving"]["version"] == "0.3.0"
        )
