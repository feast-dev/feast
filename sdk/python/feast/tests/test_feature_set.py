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
from datetime import datetime

import pytz
from unittest.mock import MagicMock
from feast.feature_set import FeatureSet, Feature
from feast.value_type import ValueType
from feast.client import Client
import pandas as pd
import pytest
from concurrent import futures
import grpc
from feast.tests.feast_core_server import CoreServicer
import feast.core.CoreService_pb2_grpc as Core
from feast.entity import Entity
from feast.source import KafkaSource
from feast.tests import dataframes

CORE_URL = "core.feast.ai"
SERVING_URL = "serving.feast.ai"


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

        # List Feature Sets
        assert (
            len(client.feature_sets) == 2
            and client.feature_sets[0].name == "my-feature-set-1"
            and client.feature_sets[0].features[0].name == "fs1-my-feature-1"
            and client.feature_sets[0].features[0].dtype == ValueType.INT64
            and client.feature_sets[1].features[1].dtype == ValueType.BYTES_LIST
        )

    @pytest.mark.parametrize("dataframe", [dataframes.GOOD])
    def test_feature_set_ingest_success(self, dataframe, client):

        # Create feature set and update based on dataframe
        driver_fs = FeatureSet("driver-feature-set")
        driver_fs.update_from_dataset(
            dataframe,
            column_mapping={"entity_id": Entity(name="entity", dtype=ValueType.INT64)},
        )
        driver_fs.source = KafkaSource(topic="feature-topic", brokers="fake.broker.com")
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
