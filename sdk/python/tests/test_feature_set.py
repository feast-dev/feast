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
from datetime import datetime

import pytz

from feast.entity import Entity
from feast.feature_set import FeatureSet, Feature
from feast.value_type import ValueType
from feast.client import Client
import pandas as pd
import pytest
from concurrent import futures
import grpc
from feast_core_server import CoreServicer
import feast.core.CoreService_pb2_grpc as Core
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

    def test_update_from_source_failure(self):
        with pytest.raises(Exception):
            df = pd.DataFrame()
            fs = FeatureSet("driver-feature-set")
            fs.infer_fields_from_df(df)

    @pytest.mark.parametrize(
        "dataframe,feature_count,entity_count,discard_unused_fields,features,entities",
        [
            (
                dataframes.GOOD,
                3,
                1,
                True,
                [],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES,
                5,
                1,
                True,
                [],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES,
                6,
                1,
                True,
                [Feature(name="feature_6", dtype=ValueType.INT64)],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES_TWO_ENTITIES,
                5,
                2,
                True,
                [],
                [
                    Entity(name="entity_1_id", dtype=ValueType.INT64),
                    Entity(name="entity_2_id", dtype=ValueType.INT64),
                ],
            ),
            (
                dataframes.GOOD_FIVE_FEATURES_TWO_ENTITIES,
                6,
                3,
                False,
                [],
                [
                    Entity(name="entity_1_id", dtype=ValueType.INT64),
                    Entity(name="entity_2_id", dtype=ValueType.INT64),
                ],
            ),
            (
                dataframes.NO_FEATURES,
                0,
                1,
                True,
                [],
                [Entity(name="entity_id", dtype=ValueType.INT64)],
            ),
            (
                pd.DataFrame(
                    {
                        "datetime": [
                            datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)
                        ]
                    }
                ),
                0,
                0,
                True,
                [],
                [],
            ),
        ],
        ids=[
            "Test small dataframe update with hardcoded entity",
            "Test larger dataframe update with hardcoded entity",
            "Test larger dataframe update with hardcoded entity and feature",
            "Test larger dataframe update with two hardcoded entities and discarding of existing fields",
            "Test larger dataframe update with two hardcoded entities and retention of existing fields",
            "Test dataframe with no featuresdataframe",
            "Test empty dataframe",
        ],
    )
    def test_add_features_from_df_success(
        self,
        dataframe,
        feature_count,
        entity_count,
        discard_unused_fields,
        features,
        entities,
    ):
        my_feature_set = FeatureSet(
            name="my_feature_set",
            features=[Feature(name="dummy_f1", dtype=ValueType.INT64)],
            entities=[Entity(name="dummy_entity_1", dtype=ValueType.INT64)],
        )
        my_feature_set.infer_fields_from_df(
            dataframe,
            discard_unused_fields=discard_unused_fields,
            features=features,
            entities=entities,
        )
        assert len(my_feature_set.features) == feature_count
        assert len(my_feature_set.entities) == entity_count
