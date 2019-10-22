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

from unittest.mock import MagicMock
from feast.feature_set import FeatureSet, Feature
from feast.value_type import ValueType
from feast.client import Client
import pandas as pd
import pytest
from concurrent import futures
import grpc
from feast_core_server import CoreServicer
import feast.core.CoreService_pb2_grpc as Core
from google.protobuf.duration_pb2 import Duration
from feast.entity import Entity
from feast.source import KafkaSource
import dataframes

CORE_URL = "core.feast.local"
SERVING_URL = "serving.feast.local"


class TestFeatureSet:
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

    def test_add_remove_features_success(self):
        fs = FeatureSet("my-feature-set")
        fs.add(Feature(name="my-feature-1", dtype=ValueType.INT64))
        fs.add(Feature(name="my-feature-2", dtype=ValueType.INT64))
        fs.drop(name="my-feature-1")
        assert len(fs.features) == 1 and fs.features[0].name == "my-feature-2"

    def test_remove_feature_failure(self):
        with pytest.raises(ValueError):
            fs = FeatureSet("my-feature-set")
            fs.drop(name="my-feature-1")

    @pytest.mark.parametrize("dataframe", [dataframes.GOOD])
    def test_update_from_source_success(self, dataframe):
        fs = FeatureSet("driver-feature-set")
        fs.update_from_dataset(
            dataframe,
            column_mapping={"entity_id": Entity(name="entity", dtype=ValueType.INT64)},
        )
        assert len(fs.features) == 3 and fs.features[1].name == "feature_2"

    def test_update_from_source_failure(self):
        with pytest.raises(Exception):
            df = pd.DataFrame()
            fs = FeatureSet("driver-feature-set")
            fs.update_from_dataset(df)

    def test_apply_feature_set(self, client):

        # Create Feature Sets
        fs1 = FeatureSet("my-feature-set-1")
        fs1.add(Feature(name="fs1-my-feature-1", dtype=ValueType.INT64))
        fs1.add(Feature(name="fs1-my-feature-2", dtype=ValueType.STRING))

        fs2 = FeatureSet("my-feature-set-2")
        fs2.add(Feature(name="fs2-my-feature-1", dtype=ValueType.STRING_LIST))
        fs2.add(Feature(name="fs2-my-feature-2", dtype=ValueType.BYTES_LIST))

        # Register Feature Set with Core
        client.apply(fs1)
        client.apply(fs2)

        feature_sets = client.list_feature_sets()

        # List Feature Sets
        assert (
            len(feature_sets) == 2
            and feature_sets[0].name == "my-feature-set-1"
            and feature_sets[0].features[0].name == "fs1-my-feature-1"
            and feature_sets[0].features[0].dtype == ValueType.INT64
            and feature_sets[1].features[1].dtype == ValueType.BYTES_LIST
        )

    @pytest.mark.parametrize("dataframe", [dataframes.GOOD])
    def test_feature_set_ingest_success(self, dataframe, client):

        driver_fs = FeatureSet("driver-feature-set")
        driver_fs.add(Feature(name="feature_1", dtype=ValueType.FLOAT))
        driver_fs.add(Feature(name="feature_2", dtype=ValueType.STRING))
        driver_fs.add(Feature(name="feature_3", dtype=ValueType.INT64))
        driver_fs.add(Entity(name="entity_id", dtype=ValueType.INT64))

        driver_fs.source = KafkaSource(topic="feature-topic", brokers="127.0.0.1")
        driver_fs._message_producer = MagicMock()
        driver_fs._message_producer.send = MagicMock()

        # Register with Feast core
        client.apply(driver_fs)

        # Ingest data into Feast
        driver_fs.ingest(dataframe=dataframe)

        # Make sure message producer is called
        driver_fs._message_producer.send.assert_called()

    @pytest.mark.parametrize(
        "dataframe,exception",
        [
            (dataframes.BAD_NO_DATETIME, Exception),
            (dataframes.BAD_INCORRECT_DATETIME_TYPE, Exception),
            (dataframes.BAD_NO_ENTITY, Exception),
            (dataframes.BAD_NO_FEATURES, Exception),
        ],
    )
    def test_feature_set_ingest_failure(self, client, dataframe, exception):
        with pytest.raises(exception):
            # Create feature set
            driver_fs = FeatureSet("driver-feature-set")
            driver_fs.source = KafkaSource(
                topic="feature-topic", brokers="fake.broker.com"
            )
            driver_fs._message_producer = MagicMock()
            driver_fs._message_producer.send = MagicMock()

            # Update based on dataset
            driver_fs.update_from_dataset(
                dataframe,
                column_mapping={
                    "entity_id": Entity(name="entity", dtype=ValueType.INT64)
                },
            )

            # Register with Feast core
            client.apply(driver_fs)

            # Ingest data into Feast
            driver_fs.ingest(dataframe=dataframe)

    @pytest.mark.parametrize("dataframe", [dataframes.ALL_TYPES])
    def test_feature_set_types_success(self, client, dataframe):

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
                Feature(name="bool_list_feature", dtype=ValueType.BOOL_LIST),
                Feature(name="double_list_feature", dtype=ValueType.DOUBLE_LIST),
            ],
            max_age=Duration(seconds=3600),
        )

        all_types_fs.source = KafkaSource(topic="feature-topic", brokers="127.0.0.1")
        all_types_fs._message_producer = MagicMock()
        all_types_fs._message_producer.send = MagicMock()

        # Register with Feast core
        client.apply(all_types_fs)

        # Ingest data into Feast
        all_types_fs.ingest(dataframe=dataframe)

        # Make sure message producer is called
        all_types_fs._message_producer.send.assert_called()
