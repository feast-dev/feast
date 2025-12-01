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


def test_multiple_join_keys():
    """Test entity with multiple join keys."""
    entity = Entity(
        name="my-entity",
        join_keys=["key1", "key2", "key3"],
        value_type=ValueType.STRING,
        description="Entity with multiple join keys"
    )

    # Test join_keys attribute
    assert entity.join_keys == ["key1", "key2", "key3"]

    # Test backward compatibility - join_key property should return first key
    assert entity.join_key == "key1"


def test_single_join_key_backward_compatibility():
    """Test that single join key still works (backward compatibility)."""
    entity = Entity(
        name="my-entity",
        join_keys=["custom-key"],
        value_type=ValueType.STRING
    )

    assert entity.join_keys == ["custom-key"]
    assert entity.join_key == "custom-key"


def test_no_join_keys_defaults_to_name():
    """Test that no join_keys defaults to [name]."""
    entity = Entity(name="my-entity", value_type=ValueType.STRING)

    assert entity.join_keys == ["my-entity"]
    assert entity.join_key == "my-entity"


def test_empty_join_keys_defaults_to_name():
    """Test that empty join_keys list defaults to [name]."""
    entity = Entity(name="my-entity", join_keys=[], value_type=ValueType.STRING)

    assert entity.join_keys == ["my-entity"]
    assert entity.join_key == "my-entity"


def test_multiple_join_keys_hash_and_equality():
    """Test hash and equality with multiple join keys."""
    entity1 = Entity(name="my-entity", join_keys=["key1", "key2"])
    entity2 = Entity(name="my-entity", join_keys=["key1", "key2"])
    entity3 = Entity(name="my-entity", join_keys=["key1", "key3"])
    entity4 = Entity(name="my-entity", join_keys=["key2", "key1"])  # Different order

    # Test equality
    assert entity1 == entity2
    assert entity1 != entity3
    assert entity1 != entity4  # Different order should be different

    # Test hash
    s1 = {entity1, entity2}
    assert len(s1) == 1  # Same entities

    s2 = {entity1, entity3}
    assert len(s2) == 2  # Different entities

    s3 = {entity1, entity4}
    assert len(s3) == 2  # Different order = different entities


def test_multiple_join_keys_protobuf_serialization():
    """Test protobuf serialization and deserialization with multiple join keys."""
    original_entity = Entity(
        name="test-entity",
        join_keys=["key1", "key2", "key3"],
        value_type=ValueType.INT64,
        description="Test entity with multiple join keys",
        tags={"env": "test"},
        owner="test-owner"
    )

    # Serialize to protobuf
    proto = original_entity.to_proto()

    # Check that both join_key and join_keys fields are populated
    assert proto.spec.join_key == "key1"  # First key for backward compatibility
    assert list(proto.spec.join_keys) == ["key1", "key2", "key3"]

    # Deserialize from protobuf
    deserialized_entity = Entity.from_proto(proto)

    # Check that deserialized entity matches original
    assert deserialized_entity.name == original_entity.name
    assert deserialized_entity.join_keys == original_entity.join_keys
    assert deserialized_entity.join_key == original_entity.join_key
    assert deserialized_entity.value_type == original_entity.value_type
    assert deserialized_entity.description == original_entity.description
    assert deserialized_entity.tags == original_entity.tags
    assert deserialized_entity.owner == original_entity.owner
    assert deserialized_entity == original_entity


def test_protobuf_backward_compatibility_single_join_key():
    """Test that protobuf with only join_key field (no join_keys) still works."""
    from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
    from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
    from feast.protos.feast.core.Entity_pb2 import EntityMeta as EntityMetaProto

    # Create a protobuf with only the old join_key field
    spec = EntitySpecProto(
        name="legacy-entity",
        value_type=ValueType.STRING.value,
        join_key="legacy-key",
        description="Legacy entity",
    )
    proto = EntityProto(spec=spec, meta=EntityMetaProto())

    # Deserialize
    entity = Entity.from_proto(proto)

    # Should work with legacy format
    assert entity.name == "legacy-entity"
    assert entity.join_keys == ["legacy-key"]
    assert entity.join_key == "legacy-key"


def test_protobuf_prioritizes_join_keys_over_join_key():
    """Test that when both join_keys and join_key are present, join_keys takes priority."""
    from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
    from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
    from feast.protos.feast.core.Entity_pb2 import EntityMeta as EntityMetaProto

    # Create a protobuf with both fields
    spec = EntitySpecProto(
        name="test-entity",
        value_type=ValueType.STRING.value,
        join_key="old-key",  # This should be ignored
        description="Test entity",
    )
    spec.join_keys.extend(["new-key1", "new-key2"])
    proto = EntityProto(spec=spec, meta=EntityMetaProto())

    # Deserialize
    entity = Entity.from_proto(proto)

    # Should use join_keys, not join_key
    assert entity.join_keys == ["new-key1", "new-key2"]
    assert entity.join_key == "new-key1"


def test_multiple_join_keys_repr():
    """Test that __repr__ shows join_keys properly."""
    entity = Entity(
        name="my-entity",
        join_keys=["key1", "key2"],
        value_type=ValueType.STRING
    )

    repr_str = repr(entity)
    assert "join_keys=['key1', 'key2']" in repr_str
    assert "name='my-entity'" in repr_str


def test_join_key_property_deprecation_warning():
    """Test that accessing join_key property triggers deprecation warning."""
    entity = Entity(
        name="my-entity",
        join_keys=["key1", "key2"],
        value_type=ValueType.STRING
    )

    # Test that accessing join_key property triggers deprecation warning
    with pytest.warns(DeprecationWarning, match="The 'join_key' property is deprecated"):
        join_key = entity.join_key
        assert join_key == "key1"


def test_legacy_protobuf_deprecation_warning():
    """Test that legacy protobuf format triggers deprecation warning."""
    from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
    from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
    from feast.protos.feast.core.Entity_pb2 import EntityMeta as EntityMetaProto

    # Create a protobuf with only the old join_key field (no join_keys)
    spec = EntitySpecProto(
        name="legacy-entity",
        value_type=ValueType.STRING.value,
        join_key="legacy-key",
        description="Legacy entity",
    )
    proto = EntityProto(spec=spec, meta=EntityMetaProto())

    # Test that deserializing legacy protobuf triggers deprecation warning
    with pytest.warns(DeprecationWarning, match="uses deprecated single join_key field"):
        entity = Entity.from_proto(proto)
        assert entity.join_keys == ["legacy-key"]


def test_join_key_property_with_no_warnings_context():
    """Test join_key property functionality without warnings interfering with other tests."""
    entity = Entity(name="my-entity", value_type=ValueType.STRING)

    # Suppress warnings for this specific test to verify functionality
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert entity.join_key == "my-entity"
        assert entity.join_keys == ["my-entity"]


def test_multiple_join_keys_no_warnings():
    """Test that new join_keys API doesn't trigger any warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # Convert all warnings to errors

        # This should not trigger any warnings
        entity = Entity(
            name="my-entity",
            join_keys=["key1", "key2", "key3"],
            value_type=ValueType.STRING
        )

        # Accessing join_keys should not trigger warnings
        assert entity.join_keys == ["key1", "key2", "key3"]

        # Only accessing join_key property should trigger warnings
        # (We'll test that separately)


def test_deprecation_warning_stacklevel():
    """Test that deprecation warnings have correct stack level for debugging."""
    entity = Entity(name="test", join_keys=["key1"], value_type=ValueType.STRING)

    # Capture warnings to check stacklevel
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = entity.join_key

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "join_key" in str(w[0].message)
        # Verify the warning points to this test function, not Entity internals
        assert "test_deprecation_warning_stacklevel" in w[0].filename
