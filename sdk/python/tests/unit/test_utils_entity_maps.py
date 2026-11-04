# Copyright 2025 The Feast Authors
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

"""
Unit tests for _get_entity_maps function in feast/utils.py

These tests verify that the fix for issue #6012 correctly eliminates
redundant registry.get_entity() calls by using a local lookup dict.

Related issue: https://github.com/feast-dev/feast/issues/6012
"""

from unittest.mock import MagicMock

from feast.entity import Entity
from feast.utils import _get_entity_maps


class MockFeatureViewProjection:
    """Mock FeatureViewProjection for testing."""

    def __init__(self, join_key_map=None):
        self.join_key_map = join_key_map or {}


class MockEntityColumn:
    """Mock entity column for testing."""

    def __init__(self, name: str, dtype):
        self.name = name
        self.dtype = dtype


class MockDtype:
    """Mock dtype with to_value_type method."""

    def __init__(self, value_type):
        self._value_type = value_type

    def to_value_type(self):
        return self._value_type


class MockFeatureView:
    """Mock FeatureView for testing."""

    def __init__(self, entities=None, entity_columns=None, join_key_map=None):
        self.entities = entities or []
        self.entity_columns = entity_columns or []
        self.projection = MockFeatureViewProjection(join_key_map)


def create_mock_entity(name: str, join_key: str) -> Entity:
    """Create a mock Entity with the specified name and join_key."""
    entity = MagicMock(spec=Entity)
    entity.name = name
    entity.join_key = join_key
    return entity


class TestGetEntityMaps:
    """Tests for _get_entity_maps function."""

    def test_no_redundant_get_entity_calls(self):
        """
        Verify that get_entity is NOT called after list_entities fetches all entities.
        This is the core fix for issue #6012.
        """
        # Create mock entities
        entity1 = create_mock_entity("driver", "driver_id")
        entity2 = create_mock_entity("customer", "customer_id")

        # Create mock registry
        registry = MagicMock()
        registry.list_entities.return_value = [entity1, entity2]

        # Create feature views that reference the entities
        fv1 = MockFeatureView(entities=["driver"])
        fv2 = MockFeatureView(entities=["customer"])
        fv3 = MockFeatureView(entities=["driver", "customer"])

        # Call the function under test
        _get_entity_maps(registry, "test_project", [fv1, fv2, fv3])

        # Verify list_entities was called once
        registry.list_entities.assert_called_once_with("test_project", allow_cache=True)

        # Verify get_entity was NEVER called (this is the fix)
        registry.get_entity.assert_not_called()

    def test_entity_name_to_join_key_mapping(self):
        """Test that entity names are correctly mapped to join keys."""
        entity1 = create_mock_entity("driver", "driver_id")
        entity2 = create_mock_entity("customer", "customer_id")

        registry = MagicMock()
        registry.list_entities.return_value = [entity1, entity2]

        fv = MockFeatureView(entities=["driver", "customer"])

        entity_name_to_join_key, _, _ = _get_entity_maps(registry, "test_project", [fv])

        assert "driver" in entity_name_to_join_key
        assert entity_name_to_join_key["driver"] == "driver_id"
        assert "customer" in entity_name_to_join_key
        assert entity_name_to_join_key["customer"] == "customer_id"

    def test_join_keys_set(self):
        """Test that the join keys set is correctly returned."""
        entity1 = create_mock_entity("driver", "driver_id")
        entity2 = create_mock_entity("customer", "customer_id")

        registry = MagicMock()
        registry.list_entities.return_value = [entity1, entity2]

        fv = MockFeatureView(entities=["driver", "customer"])

        _, _, join_keys = _get_entity_maps(registry, "test_project", [fv])

        assert "driver_id" in join_keys
        assert "customer_id" in join_keys
        assert len(join_keys) == 2

    def test_missing_entity_raises_exception(self):
        """
        Test that missing entities (not in registry) raise EntityNotFoundException.
        This maintains the original error behavior for misconfigured registries.
        """
        import pytest

        from feast.errors import EntityNotFoundException

        entity1 = create_mock_entity("driver", "driver_id")

        registry = MagicMock()
        registry.list_entities.return_value = [entity1]

        # Feature view references entity that doesn't exist in registry
        fv = MockFeatureView(entities=["driver", "nonexistent_entity"])

        # Should raise EntityNotFoundException for the missing entity
        with pytest.raises(EntityNotFoundException) as exc_info:
            _get_entity_maps(registry, "test_project", [fv])

        assert "nonexistent_entity" in str(exc_info.value)

    def test_join_key_remapping(self):
        """Test that join_key_map correctly remaps entity names and join keys."""
        entity = create_mock_entity("driver", "driver_id")

        registry = MagicMock()
        registry.list_entities.return_value = [entity]

        # Feature view with join key mapping
        fv = MockFeatureView(
            entities=["driver"],
            join_key_map={"driver_id": "remapped_driver_id"},
        )

        entity_name_to_join_key, _, join_keys = _get_entity_maps(
            registry, "test_project", [fv]
        )

        # The remapped join key should be in the mapping
        assert "remapped_driver_id" in join_keys

    def test_empty_feature_views(self):
        """Test with no feature views."""
        entity1 = create_mock_entity("driver", "driver_id")

        registry = MagicMock()
        registry.list_entities.return_value = [entity1]

        entity_name_to_join_key, entity_type_map, join_keys = _get_entity_maps(
            registry, "test_project", []
        )

        # Should still have the base entity mapping from list_entities
        assert "driver" in entity_name_to_join_key
        assert entity_name_to_join_key["driver"] == "driver_id"

    def test_empty_registry_and_feature_views(self):
        """Test with no entities and no feature views returns empty maps."""
        registry = MagicMock()
        registry.list_entities.return_value = []

        entity_name_to_join_key, entity_type_map, join_keys = _get_entity_maps(
            registry, "test_project", []
        )

        assert len(entity_name_to_join_key) == 0
        assert len(join_keys) == 0

    def test_entity_type_map_from_entity_columns(self):
        """Test that entity_type_map is populated from entity_columns."""
        from feast.value_type import ValueType

        entity = create_mock_entity("driver", "driver_id")

        registry = MagicMock()
        registry.list_entities.return_value = [entity]

        # Create entity columns with dtype
        driver_col = MockEntityColumn("driver_id", MockDtype(ValueType.INT64))
        rating_col = MockEntityColumn("rating", MockDtype(ValueType.FLOAT))

        fv = MockFeatureView(
            entities=["driver"],
            entity_columns=[driver_col, rating_col],
        )

        _, entity_type_map, _ = _get_entity_maps(registry, "test_project", [fv])

        assert "driver_id" in entity_type_map
        assert entity_type_map["driver_id"] == ValueType.INT64
        assert "rating" in entity_type_map
        assert entity_type_map["rating"] == ValueType.FLOAT


class TestGetEntityMapsPerformance:
    """Performance-related tests for _get_entity_maps."""

    def test_linear_scaling_with_feature_views(self):
        """
        Verify that increasing feature views doesn't increase registry calls.
        With N feature views referencing M entities, we should have:
        - 1 list_entities call (not N*M get_entity calls)
        """
        # Create many entities
        entities = [create_mock_entity(f"entity_{i}", f"key_{i}") for i in range(10)]

        registry = MagicMock()
        registry.list_entities.return_value = entities

        # Create many feature views, each referencing multiple entities
        feature_views = [
            MockFeatureView(entities=[f"entity_{j}" for j in range(i % 10 + 1)])
            for i in range(50)
        ]

        _get_entity_maps(registry, "test_project", feature_views)

        # Regardless of 50 feature views with varying entity counts:
        # - list_entities should be called exactly once
        # - get_entity should NEVER be called
        registry.list_entities.assert_called_once()
        registry.get_entity.assert_not_called()

    def test_duplicate_entity_references(self):
        """
        Test that duplicate entity references across feature views
        don't cause any issues or duplicate lookups.
        """
        entity = create_mock_entity("driver", "driver_id")

        registry = MagicMock()
        registry.list_entities.return_value = [entity]

        # Multiple feature views all referencing the same entity
        feature_views = [MockFeatureView(entities=["driver"]) for _ in range(20)]

        entity_name_to_join_key, _, join_keys = _get_entity_maps(
            registry, "test_project", feature_views
        )

        # Should work correctly with just one entity in the result
        assert entity_name_to_join_key["driver"] == "driver_id"
        assert "driver_id" in join_keys
        registry.get_entity.assert_not_called()
