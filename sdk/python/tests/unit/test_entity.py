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

from feast.entity import Entity
from feast.value_type import ValueType


def test_join_key_default():
    entity = Entity("my-entity", description="My entity", value_type=ValueType.STRING)
    assert entity.join_key == "my-entity"


def test_entity_class_contains_labels():
    entity = Entity(
        "my-entity",
        description="My entity",
        value_type=ValueType.STRING,
        labels={"key1": "val1", "key2": "val2"},
    )
    assert "key1" in entity.labels.keys() and entity.labels["key1"] == "val1"
    assert "key2" in entity.labels.keys() and entity.labels["key2"] == "val2"


def test_entity_without_labels_empty_dict():
    entity = Entity("my-entity", description="My entity", value_type=ValueType.STRING)
    assert entity.labels == dict()
    assert len(entity.labels) == 0


def test_entity_without_description():
    Entity("my-entity", value_type=ValueType.STRING)
