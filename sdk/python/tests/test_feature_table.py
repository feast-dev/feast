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

import socket
from concurrent import futures
from contextlib import closing

import grpc
import pytest

from feast.client import Client
from feast.core import CoreService_pb2_grpc as Core
from feast.data_format import ParquetFormat, ProtoFormat
from feast.data_source import FileSource, KafkaSource
from feast.feature import Feature
from feast.feature_table import FeatureTable
from feast.value_type import ValueType
from feast_core_server import CoreServicer


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


free_port = find_free_port()


class TestFeatureTable:
    @pytest.fixture(scope="function")
    def server(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
        server.add_insecure_port(f"[::]:{free_port}")
        server.start()
        yield server
        server.stop(0)

    @pytest.fixture
    def client(self, server):
        return Client(core_url=f"localhost:{free_port}")

    @pytest.fixture
    def batch_source(self):
        return FileSource(
            field_mapping={
                "ride_distance": "ride_distance",
                "ride_duration": "ride_duration",
            },
            file_format=ParquetFormat(),
            file_url="file://feast/*",
            event_timestamp_column="ts_col",
            created_timestamp_column="timestamp",
            date_partition_column="date_partition_col",
        )

    def test_feature_table_import_export_yaml(self, batch_source):

        stream_source = KafkaSource(
            field_mapping={
                "ride_distance": "ride_distance",
                "ride_duration": "ride_duration",
            },
            bootstrap_servers="localhost:9094",
            message_format=ProtoFormat(class_path="class.path"),
            topic="test_topic",
            event_timestamp_column="ts_col",
        )

        test_feature_table = FeatureTable(
            name="car_driver",
            features=[
                Feature(name="ride_distance", dtype=ValueType.FLOAT),
                Feature(name="ride_duration", dtype=ValueType.STRING),
            ],
            entities=["car_driver_entity"],
            labels={"team": "matchmaking"},
            batch_source=batch_source,
            stream_source=stream_source,
        )

        # Create a string YAML representation of the feature table
        string_yaml = test_feature_table.to_yaml()

        # Create a new feature table object from the YAML string
        actual_feature_table_from_string = FeatureTable.from_yaml(string_yaml)

        # Ensure equality is upheld to original feature table
        assert test_feature_table == actual_feature_table_from_string

    def test_add_feature(self, batch_source):

        test_feature_table = FeatureTable(
            name="car_driver",
            features=[
                Feature(name="ride_distance", dtype=ValueType.FLOAT),
                Feature(name="ride_duration", dtype=ValueType.STRING),
            ],
            entities=["car_driver_entity"],
            labels={"team": "matchmaking"},
            batch_source=batch_source,
        )

        test_feature_table.add_feature(
            Feature(name="new_ride_distance", dtype=ValueType.FLOAT)
        )

        features = test_feature_table.features
        assert (
            len(features) == 3
            and features[0].name == "ride_distance"
            and features[1].name == "ride_duration"
            and features[2].name == "new_ride_distance"
        )
