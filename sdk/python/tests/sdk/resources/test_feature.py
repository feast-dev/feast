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

from feast.sdk.resources.feature import Feature, Datastore, ValueType


class TestFeature(object):
    def dummy_feature(self):
        warehouse_data_store = Datastore(id="BIGQUERY1", options={})
        serving_data_store = Datastore(id="REDIS1", options={})
        my_feature = Feature(
            name="my_feature",
            entity="my_entity",
            value_type=ValueType.BYTES,
            owner="feast@web.com",
            description="test feature",
            uri="github.com/feature_repo",
            warehouse_store=warehouse_data_store,
            serving_store=serving_data_store)
        return my_feature

    def test_set_name(self):
        my_feature = self.dummy_feature()
        new_name = "my_feature_new"
        my_feature.name = new_name
        assert my_feature.name == new_name
        assert my_feature.id == "my_entity.my_feature_new"

    def test_set_entity(self):
        my_feature = self.dummy_feature()
        new_entity = "new_entity"
        my_feature.entity = new_entity
        assert my_feature.entity == new_entity
        assert my_feature.id == "new_entity.my_feature"

    def test_read_from_yaml(self):
        feature = Feature.from_yaml("tests/sample/valid_feature.yaml")
        assert feature.id == "myentity.feature_bool_redis1"
        assert feature.name == "feature_bool_redis1"
        assert feature.entity == "myentity"
        assert feature.owner == "bob@example.com"
        assert feature.description == "test entity."
        assert feature.value_type == ValueType.BOOL
        assert feature.uri == "https://github.com/bob/example"
        assert feature.serving_store.id == "REDIS1"
        assert feature.warehouse_store.id == "BIGQUERY1"
