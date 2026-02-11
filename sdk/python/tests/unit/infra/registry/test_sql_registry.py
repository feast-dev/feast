# Copyright 2021 The Feast Authors
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

import tempfile

import pytest

from feast.entity import Entity
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig


@pytest.fixture
def sqlite_registry():
    """Create a temporary SQLite registry for testing."""
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


class TestSqlRegistry:
    """Test class for SqlRegistry"""

    def test_apply_and_retrieve_entity(self, sqlite_registry):
        """Test applying and retrieving an entity from the SQL registry."""
        entity = Entity(
            name="test_entity",
            description="Test entity for testing",
            tags={"test": "transaction"},
        )
        sqlite_registry.apply_entity(entity, "test_project")

        retrieved_entity = sqlite_registry.get_entity("test_entity", "test_project")
        assert retrieved_entity.name == "test_entity"
        assert retrieved_entity.description == "Test entity for testing"

    def test_delete_entity(self, sqlite_registry):
        """Test deleting an entity from the SQL registry."""
        entity = Entity(name="test_entity", description="Test entity")
        sqlite_registry.apply_entity(entity, "test_project")

        sqlite_registry.delete_entity("test_entity", "test_project")

        with pytest.raises(Exception):
            sqlite_registry.get_entity("test_entity", "test_project")

    def test_get_project_metadata_model_returns_initialized_metadata(
        self, sqlite_registry
    ):
        """Test that get_project_metadata_model returns metadata after applying an entity."""
        entity = Entity(name="test_entity", description="Test entity")
        sqlite_registry.apply_entity(entity, "test_project")

        project_metadata = sqlite_registry.get_project_metadata_model("test_project")

        assert project_metadata.project_name == "test_project"
        assert project_metadata.project_uuid is not None
        assert project_metadata.last_updated_timestamp is not None

    def test_get_project_metadata_model_nonexistent_project(self, sqlite_registry):
        """Test that get_project_metadata_model handles non-existent projects gracefully."""
        project_metadata = sqlite_registry.get_project_metadata_model(
            "nonexistent_project"
        )

        assert project_metadata.project_name == "nonexistent_project"
        assert project_metadata is not None

    def test_get_all_project_metadata_multiple_projects(self, sqlite_registry):
        """Test that get_all_project_metadata returns metadata for all projects."""
        entity1 = Entity(name="entity1", description="Entity 1")
        entity2 = Entity(name="entity2", description="Entity 2")
        sqlite_registry.apply_entity(entity1, "project_1")
        sqlite_registry.apply_entity(entity2, "project_2")

        all_metadata = sqlite_registry.get_all_project_metadata()

        project_names = [m.project_name for m in all_metadata]
        assert "project_1" in project_names
        assert "project_2" in project_names
        for metadata in all_metadata:
            assert metadata.project_uuid is not None
