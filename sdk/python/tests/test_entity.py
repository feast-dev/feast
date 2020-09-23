# Copyright 2020 The Feast Authors
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

from collections import OrderedDict
from concurrent import futures

import grpc
import pytest

from feast.client import Client
from feast.core import CoreService_pb2_grpc as Core
from feast.entity import EntityV2
from feast.value_type import ValueType
from feast_core_server import CoreServicer

CORE_URL = "core.feast.local"
SERVING_URL = "serving.feast.local"


class TestEntity:
    @pytest.fixture(scope="function")
    def server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        server.add_insecure_port("[::]:50051")
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def client(self, server):
        return Client(core_url="localhost:50051")

    def test_entity_import_export_yaml(self):

        test_entity = EntityV2(
            name="car_driver_entity",
            description="Driver entity for car rides",
            value_type=ValueType.STRING,
            labels={"team": "matchmaking"},
        )

        # Create a string YAML representation of the entity
        string_yaml = test_entity.to_yaml()

        # Create a new entity object from the YAML string
        actual_entity_from_string = EntityV2.from_yaml(string_yaml)

        # Ensure equality is upheld to original entity
        assert test_entity == actual_entity_from_string


def test_entity_class_contains_labels():
    entity = EntityV2(
        "my-entity",
        description="My entity",
        value_type=ValueType.STRING,
        labels={"key1": "val1", "key2": "val2"},
    )
    assert "key1" in entity.labels.keys() and entity.labels["key1"] == "val1"
    assert "key2" in entity.labels.keys() and entity.labels["key2"] == "val2"


def test_entity_without_labels_empty_dict():
    entity = EntityV2("my-entity", description="My entity", value_type=ValueType.STRING)
    assert entity.labels == OrderedDict()
    assert len(entity.labels) == 0
