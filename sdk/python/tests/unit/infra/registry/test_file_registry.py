import tempfile
from datetime import timedelta

import pytest

from feast import Field
from feast.batch_feature_view import BatchFeatureView
from feast.data_source import PushSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteTable
from feast.infra.registry.registry import Registry
from feast.project import Project
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


@pytest.mark.parametrize("mode", ["pandas", "sql", "flink"])
def test_apply_batch_feature_view_stores_changed_udf(file_registry, mode):
    """Re-applying a BatchFeatureView with a new UDF must store the new UDF."""
    entity = Entity(name="driver", join_keys=["driver_id"])
    file_registry.apply_entity(entity, "test_project")
    file_source, _ = _make_sources()

    def make_view(udf, udf_string):
        return BatchFeatureView(
            name="driver_bfv",
            entities=[entity],
            mode=mode,
            source=file_source,
            schema=[Field(name="out", dtype=Float32)],
            ttl=timedelta(days=1),
            udf=udf,
            udf_string=udf_string,
        )

    def udf_a(df):
        return df

    def udf_b(df):
        return df

    view_a = make_view(udf_a, "udf_a source")
    file_registry.apply_feature_view(view_a, "test_project")

    # Re-applying the same definition works and stays a metadata-only update.
    file_registry.apply_feature_view(make_view(udf_a, "udf_a source"), "test_project")
    stored_a = BatchFeatureView.from_proto(view_a.to_proto())
    assert not view_a._schema_or_udf_changed(stored_a)

    file_registry.apply_feature_view(make_view(udf_b, "udf_b source"), "test_project")

    stored = file_registry.get_feature_view("driver_bfv", "test_project")
    assert type(stored.feature_transformation) is type(view_a.feature_transformation)
    assert stored.feature_transformation.udf_string == "udf_b source"


@pytest.mark.parametrize("change", ["constant", "default", "closure"])
def test_reapply_batch_feature_view_detects_udf_change_without_source(
    file_registry, change
):
    """With an empty udf_string, a UDF change that bytecode alone misses is stored."""
    entity = Entity(name="driver", join_keys=["driver_id"])
    file_registry.apply_entity(entity, "test_project")
    file_source, _ = _make_sources()

    def with_default(query):
        def udf(_, query=query):
            return query

        return udf

    def with_closure(query):
        return lambda _: query

    old_udf, new_udf = {
        "constant": (lambda _: "SELECT 1", lambda _: "SELECT 2"),
        "default": (with_default("SELECT 1"), with_default("SELECT 2")),
        "closure": (with_closure("SELECT 1"), with_closure("SELECT 2")),
    }[change]

    def make_view(udf):
        return BatchFeatureView(
            name="driver_bfv",
            entities=[entity],
            mode="sql",
            source=file_source,
            schema=[Field(name="out", dtype=Float32)],
            ttl=timedelta(days=1),
            udf=udf,
            udf_string="",
        )

    old_view = make_view(old_udf)
    file_registry.apply_feature_view(old_view, "test_project")

    # Re-applying the same UDF works and stays a metadata-only update.
    file_registry.apply_feature_view(make_view(old_udf), "test_project")
    assert not old_view._schema_or_udf_changed(
        BatchFeatureView.from_proto(old_view.to_proto())
    )

    file_registry.apply_feature_view(make_view(new_udf), "test_project")

    stored = file_registry.get_feature_view("driver_bfv", "test_project")
    assert stored.feature_transformation.udf(None) == "SELECT 2"


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


def test_delete_project_removes_only_its_infra_objects(file_registry, tmp_path):
    online_store_path = str(tmp_path / "online.db")
    infra = Infra(
        infra_objects=[
            SqliteTable(path=online_store_path, name="project_a_driver_stats"),
            SqliteTable(path=online_store_path, name="project_b_driver_stats"),
        ]
    )
    file_registry.apply_project(Project(name="project_a"))
    file_registry.apply_project(Project(name="project_b"))
    file_registry.update_infra(infra, "project_a")

    file_registry.delete_project("project_a")

    remaining = file_registry.get_infra("project_b").infra_objects
    assert [infra_object.name for infra_object in remaining] == [
        "project_b_driver_stats"
    ]
