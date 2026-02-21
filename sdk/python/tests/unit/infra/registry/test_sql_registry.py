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
from datetime import timedelta

import pytest

from feast import Field
from feast.data_source import PushSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import Float32
from feast.value_type import ValueType


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


def test_sql_registry(sqlite_registry):
    """
    Test the SQL registry
    """
    entity = Entity(
        name="test_entity",
        description="Test entity for testing",
        tags={"test": "transaction"},
    )
    sqlite_registry.apply_entity(entity, "test_project")
    retrieved_entity = sqlite_registry.get_entity("test_entity", "test_project")
    assert retrieved_entity.name == "test_entity"
    assert retrieved_entity.description == "Test entity for testing"

    sqlite_registry.set_project_metadata("test_project", "test_key", "test_value")
    value = sqlite_registry.get_project_metadata("test_project", "test_key")
    assert value == "test_value"

    sqlite_registry.delete_entity("test_entity", "test_project")
    with pytest.raises(Exception):
        sqlite_registry.get_entity("test_entity", "test_project")


def _build_feature_view(name: str, entity: Entity, source: FileSource) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=source,
    )


def test_feature_view_name_conflict_between_stream_and_batch(sqlite_registry):
    entity = Entity(
        name="driver",
        value_type=ValueType.STRING,
        join_keys=["driver_id"],
    )
    sqlite_registry.apply_entity(entity, "test_project")

    file_source = FileSource(
        path="driver_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )

    batch_view = _build_feature_view("driver_activity", entity, file_source)
    sqlite_registry.apply_feature_view(batch_view, "test_project")

    push_source = PushSource(name="driver_push", batch_source=file_source)
    stream_view = StreamFeatureView(
        name="driver_activity",
        source=push_source,
        entities=[entity],
        schema=[Field(name="conv_rate", dtype=Float32)],
        timestamp_field="event_timestamp",
    )

    with pytest.raises(ConflictingFeatureViewNames):
        sqlite_registry.apply_feature_view(stream_view, "test_project")
