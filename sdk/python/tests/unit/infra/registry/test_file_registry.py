import tempfile
from datetime import timedelta

import pytest

from feast import Field
from feast.batch_feature_view import BatchFeatureView
from feast.data_source import PushSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames, FeatureViewPinConflict
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


def test_pin_batch_feature_view_rejects_changed_udf(file_registry):
    """Pinning a version while also changing the UDF raises instead of dropping it."""
    entity = Entity(name="driver", join_keys=["driver_id"])
    file_registry.apply_entity(entity, "test_project")
    file_source, _ = _make_sources()

    def make_view(udf, udf_string, version="latest"):
        return BatchFeatureView(
            name="driver_bfv",
            entities=[entity],
            mode="pandas",
            source=file_source,
            schema=[Field(name="out", dtype=Float32)],
            ttl=timedelta(days=1),
            udf=udf,
            udf_string=udf_string,
            version=version,
        )

    def udf_a(df):
        return df

    def udf_b(df):
        return df

    def udf_c(df):
        return df

    file_registry.apply_feature_view(make_view(udf_a, "udf_a source"), "test_project")
    file_registry.apply_feature_view(make_view(udf_b, "udf_b source"), "test_project")

    with pytest.raises(FeatureViewPinConflict):
        file_registry.apply_feature_view(
            make_view(udf_c, "udf_c source", version="v0"), "test_project"
        )

    # Pinning with the active definition unchanged still restores v0.
    file_registry.apply_feature_view(
        make_view(udf_b, "udf_b source", version="v0"), "test_project"
    )
    stored = file_registry.get_feature_view("driver_bfv", "test_project")
    assert stored.feature_transformation.udf_string == "udf_a source"


_UDF_FACTORIES = """
import numpy as np


def make_query_udf(query):
    def udf(df):
        return " ".join(word for word in query.split())

    return udf


def make_array_udf(values):
    weights = np.array(values)

    def udf(df):
        return df * weights

    return udf
"""


_SOURCE_EDITS = {
    "none": lambda source: source,
    # A line added above the factories moves every UDF down.
    "moved": lambda source: "\n" + source,
    # A blank line inside the UDF body changes only its line table.
    "reformatted": lambda source: source.replace("(df):\n", "(df):\n\n"),
}


def _load_udf(factory, arg, edit="none"):
    """Build a UDF from a module that is not importable, so dill stores it by value."""
    namespace = {"__name__": "feature_definitions_not_importable"}
    source = _SOURCE_EDITS[edit](_UDF_FACTORIES)
    exec(compile(source, "feature_definitions.py", "exec"), namespace)
    return namespace[factory](arg)


@pytest.mark.parametrize(
    "factory, arg, edit, changed",
    [
        ("make_query_udf", "SELECT 1", "moved", False),
        ("make_query_udf", "SELECT 1", "reformatted", False),
        ("make_query_udf", "SELECT 2", "moved", True),
        ("make_array_udf", [1, 2], "none", False),
        ("make_array_udf", [1, 3], "none", True),
    ],
    ids=["moved", "reformatted", "moved_and_edited", "same_array", "edited_array"],
)
def test_pin_batch_feature_view_compares_udf_without_source(
    file_registry, factory, arg, edit, changed
):
    """Without a udf_string, moving the UDF or capturing an array is not an edit."""
    entity = Entity(name="driver", join_keys=["driver_id"])
    file_registry.apply_entity(entity, "test_project")
    file_source, _ = _make_sources()

    def make_view(udf, version="latest"):
        return BatchFeatureView(
            name="driver_bfv",
            entities=[entity],
            mode="pandas",
            source=file_source,
            schema=[Field(name="out", dtype=Float32)],
            ttl=timedelta(days=1),
            udf=udf,
            udf_string="",
            version=version,
        )

    base_arg = {"make_query_udf": "SELECT 1", "make_array_udf": [1, 2]}[factory]
    file_registry.apply_feature_view(
        make_view(_load_udf("make_query_udf", "SELECT 0")), "test_project"
    )
    file_registry.apply_feature_view(
        make_view(_load_udf(factory, base_arg)), "test_project"
    )

    # The version check and the pin check agree on what counts as an edit.
    active = file_registry.get_feature_view("driver_bfv", "test_project")
    candidate = make_view(_load_udf(factory, arg, edit))
    assert candidate._schema_or_udf_changed(active) is changed

    pinned = make_view(_load_udf(factory, arg, edit), version="v0")
    if changed:
        with pytest.raises(FeatureViewPinConflict):
            file_registry.apply_feature_view(pinned, "test_project")
    else:
        file_registry.apply_feature_view(pinned, "test_project")
        stored = file_registry.get_feature_view("driver_bfv", "test_project")
        assert stored.current_version_number == 0
        assert stored.feature_transformation.udf(None) == "SELECT 0"


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
