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
import assertpy
import pytest

from feast.entity import Entity
from feast.value_type import ValueType


def test_join_key_default():
    with pytest.deprecated_call():
        entity = Entity(
            "my-entity", description="My entity", value_type=ValueType.STRING
        )
    assert entity.join_key == "my-entity"


def test_entity_class_contains_tags():
    with pytest.deprecated_call():
        entity = Entity(
            "my-entity",
            description="My entity",
            value_type=ValueType.STRING,
            tags={"key1": "val1", "key2": "val2"},
        )
    assert "key1" in entity.tags.keys() and entity.tags["key1"] == "val1"
    assert "key2" in entity.tags.keys() and entity.tags["key2"] == "val2"


def test_entity_without_tags_empty_dict():
    with pytest.deprecated_call():
        entity = Entity(
            "my-entity", description="My entity", value_type=ValueType.STRING
        )
    assert entity.tags == dict()
    assert len(entity.tags) == 0


def test_entity_without_description():
    with pytest.deprecated_call():
        Entity("my-entity", value_type=ValueType.STRING)


def test_name_not_specified():
    assertpy.assert_that(lambda: Entity(value_type=ValueType.STRING)).raises(ValueError)


def test_multiple_args():
    assertpy.assert_that(lambda: Entity("a", ValueType.STRING)).raises(ValueError)


def test_name_keyword(recwarn):
    Entity(name="my-entity", value_type=ValueType.STRING)
    assert len(recwarn) == 0


def test_hash():
    entity1 = Entity(name="my-entity", value_type=ValueType.STRING)
    entity2 = Entity(name="my-entity", value_type=ValueType.STRING)
    entity3 = Entity(name="my-entity", value_type=ValueType.FLOAT)
    entity4 = Entity(name="my-entity", value_type=ValueType.FLOAT, description="test")

    s1 = {entity1, entity2}
    assert len(s1) == 1

    s2 = {entity1, entity3}
    assert len(s2) == 2

    s3 = {entity3, entity4}
    assert len(s3) == 2

    s4 = {entity1, entity2, entity3, entity4}
    assert len(s4) == 3
