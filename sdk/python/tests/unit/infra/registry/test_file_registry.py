import os
import tempfile
import threading
from datetime import timedelta
from unittest.mock import patch

import pytest

from feast import Field
from feast.batch_feature_view import BatchFeatureView
from feast.data_source import PushSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.registry import Registry
from feast.project import Project
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import Float32
from feast.utils import _utc_now
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


def test_cached_read_does_not_wait_for_in_progress_refresh(
    file_registry: Registry,
) -> None:
    """A read the cache can serve must not wait for a refresh in progress."""
    results: list[list[Project]] = []

    def cached_read() -> None:
        results.append(file_registry.list_projects(allow_cache=True))

    thread = threading.Thread(target=cached_read, daemon=True)

    # Hold the lock to simulate a refresh stuck downloading in another thread.
    file_registry._refresh_lock.acquire()
    try:
        thread.start()
        thread.join(timeout=2)
        assert not thread.is_alive(), "cached read waited for the refresh lock"
    finally:
        file_registry._refresh_lock.release()
        thread.join(timeout=5)

    assert results == [[]]


def test_expired_cache_read_fetches_from_store(file_registry: Registry) -> None:
    """Once the TTL has passed, a cached read must download a fresh registry."""
    fresh_proto = RegistryProto()
    fresh_proto.projects.append(Project(name="fresh_project").to_proto())
    file_registry.cached_registry_proto_ttl = timedelta(seconds=1)
    file_registry.cached_registry_proto_created = _utc_now() - timedelta(seconds=5)

    with patch.object(
        file_registry._registry_store,
        "get_registry_proto",
        return_value=fresh_proto,
    ):
        projects = file_registry.list_projects(allow_cache=True)

    assert [p.name for p in projects] == ["fresh_project"]


def test_cached_read_waits_for_reload_of_modified_local_file(
    file_registry: Registry,
) -> None:
    """A cached read during a reload of a changed file must wait for the new snapshot."""
    registry_path = file_registry._file_path
    last_read_mtime = file_registry._file_mtime
    assert registry_path is not None
    assert last_read_mtime is not None

    # Change the registry file and move its mtime past the last read.
    writer = Registry("test_project", RegistryConfig(path=str(registry_path)), None)
    writer.apply_project(Project(name="written_project"), commit=True)
    future_mtime = last_read_mtime + 10
    os.utime(registry_path, (future_mtime, future_mtime))

    reload_started = threading.Event()
    release_reload = threading.Event()
    real_get_registry_proto = file_registry._registry_store.get_registry_proto
    results: dict[str, list[Project]] = {}

    def paused_get_registry_proto() -> RegistryProto:
        reload_started.set()
        release_reload.wait(timeout=10)
        return real_get_registry_proto()

    def cached_read(name: str) -> None:
        results[name] = file_registry.list_projects(allow_cache=True)

    reloading_read = threading.Thread(
        target=cached_read, args=("reloading",), daemon=True
    )
    concurrent_read = threading.Thread(
        target=cached_read, args=("concurrent",), daemon=True
    )

    # Pause the reload inside get_registry_proto and start a second read meanwhile.
    with patch.object(
        file_registry._registry_store,
        "get_registry_proto",
        side_effect=paused_get_registry_proto,
    ):
        reloading_read.start()
        try:
            assert reload_started.wait(timeout=5), "reload did not start"
            concurrent_read.start()
            concurrent_read.join(timeout=0.5)
            assert concurrent_read.is_alive(), "cached read returned during reload"
        finally:
            release_reload.set()
            reloading_read.join(timeout=5)
    concurrent_read.join(timeout=5)

    assert [p.name for p in results["reloading"]] == ["written_project"]
    assert [p.name for p in results["concurrent"]] == ["written_project"]
