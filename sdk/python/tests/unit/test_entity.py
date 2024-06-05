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
from datetime import datetime, timedelta

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


def test_update_meta_with_entity():
    # Create an entity that is already present in the SQL registry
    stored_entity = Entity(
        name="my-entity", join_keys=["key"], value_type=ValueType.INT32
    )
    current_time = datetime.now()
    stored_entity.created_timestamp = current_time - timedelta(days=1)
    stored_entity.last_updated_timestamp = current_time - timedelta(days=1)
    stored_entity_proto = stored_entity.to_proto()
    serialized_proto = stored_entity_proto.SerializeToString()

    # Update the entity i.e. here it's simply the name
    updated_entity = Entity(
        name="my-entity-1", join_keys=["key"], value_type=ValueType.INT32
    )
    updated_entity.last_updated_timestamp = current_time

    updated_entity.update_meta(serialized_proto)
    assert updated_entity.created_timestamp == stored_entity.created_timestamp
    assert updated_entity.last_updated_timestamp == current_time
