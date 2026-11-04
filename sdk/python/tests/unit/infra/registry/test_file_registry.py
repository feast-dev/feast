import tempfile
from datetime import timedelta

import pytest

from feast import Field
from feast.data_source import PushSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import Float32
from feast.value_type import ValueType


@pytest.fixture
def file_registry():
    fd, registry_path = tempfile.mkstemp()
    config = RegistryConfig(path=registry_path)
    registry = Registry("test_project", config, None)
    yield registry
    registry.teardown()


def _make_sources():
    file_source = FileSource(
        path="driver_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    push_source = PushSource(name="driver_push", batch_source=file_source)
    return file_source, push_source


def test_same_project_name_conflict_batch_vs_stream(file_registry):
    """A FeatureView and StreamFeatureView with the same name in the same project must raise ConflictingFeatureViewNames."""
    entity = Entity(name="driver", value_type=ValueType.STRING, join_keys=["driver_id"])
    file_registry.apply_entity(entity, "test_project")

    file_source, push_source = _make_sources()

    batch_view = FeatureView(
        name="driver_activity",
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=file_source,
    )
    file_registry.apply_feature_view(batch_view, "test_project")

    stream_view = StreamFeatureView(
        name="driver_activity",
        source=push_source,
        entities=[entity],
        schema=[Field(name="conv_rate", dtype=Float32)],
        timestamp_field="event_timestamp",
    )
    with pytest.raises(ConflictingFeatureViewNames):
        file_registry.apply_feature_view(stream_view, "test_project")


def test_cross_project_name_does_not_conflict_batch_vs_stream(file_registry):
    """A FeatureView in project_a and a StreamFeatureView with the same name in project_b
    must not raise ConflictingFeatureViewNames.

    Before the fix, _existing_feature_view_names_to_fvs scanned all projects,
    so the type mismatch between the two projects triggered a spurious error.
    """
    entity = Entity(name="driver", value_type=ValueType.STRING, join_keys=["driver_id"])
    file_registry.apply_entity(entity, "project_a")
    file_registry.apply_entity(entity, "project_b")

    file_source, push_source = _make_sources()

    batch_view = FeatureView(
        name="driver_activity",
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=file_source,
    )
    file_registry.apply_feature_view(batch_view, "project_a")

    stream_view = StreamFeatureView(
        name="driver_activity",
        source=push_source,
        entities=[entity],
        schema=[Field(name="conv_rate", dtype=Float32)],
        timestamp_field="event_timestamp",
    )
    # Must not raise — same name, different project, different type.
    file_registry.apply_feature_view(stream_view, "project_b")
