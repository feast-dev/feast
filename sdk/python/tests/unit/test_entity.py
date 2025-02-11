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
import warnings

import assertpy
import pytest

from feast.entity import Entity
from feast.value_type import ValueType


def test_join_key_default():
    entity = Entity(name="my-entity", description="My entity")
    assert entity.join_key == "my-entity"


def test_entity_class_contains_tags():
    entity = Entity(
        name="my-entity",
        description="My entity",
        tags={"key1": "val1", "key2": "val2"},
    )
    assert "key1" in entity.tags.keys() and entity.tags["key1"] == "val1"
    assert "key2" in entity.tags.keys() and entity.tags["key2"] == "val2"


def test_entity_without_tags_empty_dict():
    entity = Entity(name="my-entity", description="My entity")
    assert entity.tags == dict()
    assert len(entity.tags) == 0


def test_entity_without_description():
    _ = Entity(name="my-entity")


def test_entity_without_name():
    with pytest.raises(TypeError):
        _ = Entity()


def test_name_not_specified():
    assertpy.assert_that(lambda: Entity()).raises(ValueError)


def test_multiple_args():
    assertpy.assert_that(lambda: Entity("a", ValueType.STRING)).raises(ValueError)


def test_hash():
    entity1 = Entity(name="my-entity")
    entity2 = Entity(name="my-entity")
    entity3 = Entity(name="my-entity", join_keys=["not-my-entity"])
    entity4 = Entity(name="my-entity", join_keys=["not-my-entity"], description="test")

    s1 = {entity1, entity2}
    assert len(s1) == 1

    s2 = {entity1, entity3}
    assert len(s2) == 2

    s3 = {entity3, entity4}
    assert len(s3) == 2

    s4 = {entity1, entity2, entity3, entity4}
    assert len(s4) == 3


def test_entity_without_value_type_warns():
    with pytest.warns(DeprecationWarning, match="Entity value_type will be mandatory"):
        entity = Entity(name="my-entity")
    assert entity.value_type == ValueType.UNKNOWN


def test_entity_with_value_type_no_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        entity = Entity(name="my-entity", value_type=ValueType.STRING)
    assert entity.value_type == ValueType.STRING
