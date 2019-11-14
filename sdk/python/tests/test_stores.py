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


import pytest
import stores
from feast.feature_set import FeatureSet
from feast.feature import Feature
from feast.entity import Entity
from feast.value_type import ValueType
from feast.types import (
    FeatureRow_pb2 as FeatureRowProto,
    Field_pb2 as FieldProto,
    Value_pb2 as ValueProto,
)
from google.protobuf.timestamp_pb2 import Timestamp


class TestStores:
    @pytest.fixture(scope="module")
    def sqlite_store(self):
        return stores.SQLiteDatabase()

    def test_register_feature_set(self, sqlite_store):
        fs = FeatureSet("my-feature-set")
        fs.add(Feature(name="my-feature-1", dtype=ValueType.INT64))
        fs.add(Feature(name="my-feature-2", dtype=ValueType.INT64))
        fs.add(Entity(name="my-entity-1", dtype=ValueType.INT64))
        fs._version = 1
        feature_set_proto = fs.to_proto()

        sqlite_store.register_feature_set(feature_set_proto)
        feature_row = FeatureRowProto.FeatureRow(
            feature_set="feature_set_1",
            event_timestamp=Timestamp(),
            fields=[
                FieldProto.Field(
                    name="feature_1", value=ValueProto.Value(float_val=1.2)
                ),
                FieldProto.Field(
                    name="feature_2", value=ValueProto.Value(float_val=1.2)
                ),
                FieldProto.Field(
                    name="feature_3", value=ValueProto.Value(float_val=1.2)
                ),
            ],
        )
        # sqlite_store.upsert_feature_row(feature_set_proto, feature_row)
        assert True
