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
import pathlib
from concurrent import futures
from datetime import datetime

import grpc
import pandas as pd
import pytest
import pytz
from google.protobuf import json_format

import dataframes
import feast.core.CoreService_pb2_grpc as Core
from feast.client import Client
from feast.entity import Entity
from feast.feature_set import (
    Feature,
    FeatureSet,
    FeatureSetRef,
    _make_tfx_schema_domain_info_inline,
)
from feast.value_type import ValueType
from feast_core_server import CoreServicer
from tensorflow_metadata.proto.v0 import schema_pb2

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

    def test_import_tfx_schema(self):
        tests_folder = pathlib.Path(__file__).parent
        test_input_schema_json = open(
            tests_folder / "data" / "tensorflow_metadata" / "bikeshare_schema.json"
        ).read()
        test_input_schema = schema_pb2.Schema()
        json_format.Parse(test_input_schema_json, test_input_schema)

        feature_set = FeatureSet(
            name="bikeshare",
            entities=[Entity(name="station_id", dtype=ValueType.INT64)],
            features=[
                Feature(name="name", dtype=ValueType.STRING),
                Feature(name="status", dtype=ValueType.STRING),
                Feature(name="latitude", dtype=ValueType.FLOAT),
                Feature(name="longitude", dtype=ValueType.FLOAT),
                Feature(name="location", dtype=ValueType.STRING),
            ],
        )

        # Before update
        for entity in feature_set.entities:
            assert entity.presence is None
            assert entity.shape is None
        for feature in feature_set.features:
            assert feature.presence is None
            assert feature.shape is None
            assert feature.string_domain is None
            assert feature.float_domain is None
            assert feature.int_domain is None

        feature_set.import_tfx_schema(test_input_schema)

        # After update
        for feature in feature_set.features:
            assert feature.presence is not None
            assert feature.shape is not None
            if feature.name in ["location", "name", "status"]:
                assert feature.string_domain is not None
            elif feature.name in ["latitude", "longitude"]:
                assert feature.float_domain is not None
            elif feature.name in ["station_id"]:
                assert feature.int_domain is not None

    def test_export_tfx_schema(self):
        tests_folder = pathlib.Path(__file__).parent
        test_input_feature_set = FeatureSet.from_yaml(
            str(
                tests_folder
                / "data"
                / "tensorflow_metadata"
                / "bikeshare_feature_set.yaml"
            )
        )

        expected_schema_json = open(
            tests_folder / "data" / "tensorflow_metadata" / "bikeshare_schema.json"
        ).read()
        expected_schema = schema_pb2.Schema()
        json_format.Parse(expected_schema_json, expected_schema)
        _make_tfx_schema_domain_info_inline(expected_schema)

        actual_schema = test_input_feature_set.export_tfx_schema()

        assert len(actual_schema.feature) == len(expected_schema.feature)
        for actual, expected in zip(actual_schema.feature, expected_schema.feature):
            assert actual.SerializeToString() == expected.SerializeToString()


def make_tfx_schema_domain_info_inline(schema):
    # Copy top-level domain info defined in the schema to inline definition.
    # One use case is in FeatureSet which does not have access to the top-level domain
    # info.
    domain_ref_to_string_domain = {d.name: d for d in schema.string_domain}
    domain_ref_to_float_domain = {d.name: d for d in schema.float_domain}
    domain_ref_to_int_domain = {d.name: d for d in schema.int_domain}

    for feature in schema.feature:
        domain_info_case = feature.WhichOneof("domain_info")
        if domain_info_case == "domain":
            domain_ref = feature.domain
            if domain_ref in domain_ref_to_string_domain:
                feature.string_domain.MergeFrom(domain_ref_to_string_domain[domain_ref])
            elif domain_ref in domain_ref_to_float_domain:
                feature.float_domain.MergeFrom(domain_ref_to_float_domain[domain_ref])
            elif domain_ref in domain_ref_to_int_domain:
                feature.int_domain.MergeFrom(domain_ref_to_int_domain[domain_ref])


class TestFeatureSetRef:
    def test_from_feature_set(self):
        feature_set = FeatureSet("test", "test")
        ref = FeatureSetRef.from_feature_set(feature_set)

        assert ref.name == "test"
        assert ref.project == "test"

    def test_str_ref(self):
        original_ref = FeatureSetRef(project="test", name="test")
        ref_str = repr(original_ref)
        parsed_ref = FeatureSetRef.from_str(ref_str)
        assert original_ref == parsed_ref
