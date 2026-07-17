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

import logging
import sys
import tempfile
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import dill
import pytest

from feast import Field
from feast.data_source import PushSource
from feast.entity import Entity
from feast.errors import ConflictingFeatureViewNames
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.registry.sql import (
    ProtoBytes,
    SqlRegistry,
    SqlRegistryConfig,
    feature_views,
)
from feast.protos.feast.core.Transformation_pb2 import (
    FeatureTransformationV2,
    UserDefinedFunctionV2,
)
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


@pytest.fixture
def shared_sqlite_db_path():
    """Return a shared SQLite DB path for cross-project tests."""
    fd, path = tempfile.mkstemp()
    yield path


def test_proto_columns_use_longblob_on_mysql():
    """On MySQL and MariaDB, serialized-proto columns must compile to LONGBLOB.

    MySQL maps SQLAlchemy's LargeBinary to BLOB, which caps at 64 KB and
    silently truncates larger protos (e.g. a FeatureView proto), later
    surfacing as a protobuf DecodeError. The dialect-aware ProtoBytes type
    must emit LONGBLOB so large protos round-trip intact. MariaDB is checked
    separately because SQLAlchemy 2.x reports dialect.name == "mariadb", which
    would miss a "mysql"-only variant.

    NOTE: this asserts only the compiled DDL type. The live >64 KB proto
    round-trip through a real MySQL/MariaDB connection is covered by
    test_apply_feature_view_large_proto_roundtrip_mysql in
    tests/integration/registration/test_universal_registry.py.
    """
    from sqlalchemy.dialects import mysql, postgresql, sqlite
    from sqlalchemy.schema import CreateTable

    # The MariaDB dialect lives at a SQLAlchemy-version-specific path; import it
    # defensively so the rest of this test still runs on SQLAlchemy 1.4.x (a
    # version the production code and dependency floor, >=1.4.19, support). The
    # always-present mysql dialect keeps the LONGBLOB assertion unconditional.
    try:
        from sqlalchemy.dialects.mysql.mariadb import MariaDBDialect

        mariadb_dialect = MariaDBDialect()
    except ImportError:  # pragma: no cover - only on SQLAlchemy builds w/o MariaDB
        mariadb_dialect = None

    # ProtoBytes columns share the same MetaData as every registry table, so
    # reach it through an already-imported table instead of a second import.
    registry_metadata = feature_views.metadata

    # Every column whose name ends in these suffixes stores a (potentially
    # large) serialized proto or its companion metadata blob.
    binary_suffixes = ("_proto", "_intervals", "user_metadata")

    checked = 0
    for table in registry_metadata.tables.values():
        binary_columns = [
            c.name for c in table.columns if c.name.endswith(binary_suffixes)
        ]
        # Guard against drift between this name-suffix heuristic and the
        # production type predicate (`column.type is ProtoBytes`) used by the
        # startup diagnostic: every suffix-matched column must be typed
        # ProtoBytes, and no ProtoBytes column may be missed by the suffix list.
        # Catches a proto column accidentally typed as plain LargeBinary.
        proto_typed_columns = [c.name for c in table.columns if c.type is ProtoBytes]
        assert set(binary_columns) == set(proto_typed_columns), (
            f"{table.name}: name-suffix vs ProtoBytes-typed columns disagree — "
            f"suffix={sorted(binary_columns)} typed={sorted(proto_typed_columns)}"
        )
        if not binary_columns:
            continue

        mysql_ddl = str(CreateTable(table).compile(dialect=mysql.dialect()))
        sqlite_ddl = str(CreateTable(table).compile(dialect=sqlite.dialect()))
        postgres_ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        mariadb_ddl = (
            str(CreateTable(table).compile(dialect=mariadb_dialect))
            if mariadb_dialect is not None
            else None
        )

        for col_name in binary_columns:
            # MySQL and MariaDB must use LONGBLOB for the column (not bare BLOB).
            assert f"{col_name} LONGBLOB" in mysql_ddl, (
                f"{table.name}.{col_name} should compile to LONGBLOB on MySQL, "
                f"got:\n{mysql_ddl}"
            )
            if mariadb_ddl is not None:
                assert f"{col_name} LONGBLOB" in mariadb_ddl, (
                    f"{table.name}.{col_name} should compile to LONGBLOB on "
                    f"MariaDB, got:\n{mariadb_ddl}"
                )
            # Other dialects keep LargeBinary's default mapping: BLOB on SQLite,
            # BYTEA on PostgreSQL.
            assert f"{col_name} BLOB" in sqlite_ddl, (
                f"{table.name}.{col_name} should compile to BLOB on SQLite, "
                f"got:\n{sqlite_ddl}"
            )
            assert f"{col_name} BYTEA" in postgres_ddl, (
                f"{table.name}.{col_name} should compile to BYTEA on PostgreSQL, "
                f"got:\n{postgres_ddl}"
            )
            checked += 1

    assert checked > 0, "expected at least one serialized-proto column to check"


def _mock_mysql_engine(dialect_name, stale_rows):
    """Build a MagicMock Engine that returns ``stale_rows`` from the BLOB query."""
    engine = MagicMock()
    engine.dialect.name = dialect_name
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = stale_rows
    return engine


@pytest.mark.parametrize("dialect_name", ["mysql", "mariadb"])
def test_warn_if_narrow_blob_columns_errors_on_stale(caplog, dialect_name):
    """On MySQL/MariaDB, stale BLOB registry columns log an error at startup."""
    engine = _mock_mysql_engine(
        dialect_name,
        [("feature_views", "feature_view_proto"), ("entities", "entity_proto")],
    )

    with caplog.at_level(logging.ERROR):
        SqlRegistry._warn_if_narrow_blob_columns(engine)

    assert "still typed BLOB" in caplog.text
    assert "feature_views.feature_view_proto" in caplog.text
    assert "entities.entity_proto" in caplog.text
    # Reported at ERROR so monitoring that filters below ERROR still catches it.
    assert any(r.levelno == logging.ERROR for r in caplog.records)


@pytest.mark.parametrize("dialect_name", ["mysql", "mariadb"])
def test_warn_if_narrow_blob_columns_silent_when_migrated(caplog, dialect_name):
    """A fully-migrated MySQL/MariaDB registry (no BLOB columns) stays silent."""
    engine = _mock_mysql_engine(dialect_name, [])

    with caplog.at_level(logging.ERROR):
        SqlRegistry._warn_if_narrow_blob_columns(engine)

    assert "still typed BLOB" not in caplog.text


def test_warn_if_narrow_blob_columns_does_not_refuse_to_start(caplog):
    """Stale columns are reported but must never raise — an upgrade can't break
    a registry whose protos all fit in 64 KB."""
    engine = _mock_mysql_engine("mysql", [("feature_views", "feature_view_proto")])

    # Must not raise.
    SqlRegistry._warn_if_narrow_blob_columns(engine)


def test_warn_if_narrow_blob_columns_skips_non_mysql():
    """Non-MySQL/MariaDB dialects skip the information_schema query entirely."""
    engine = MagicMock()
    engine.dialect.name = "sqlite"

    SqlRegistry._warn_if_narrow_blob_columns(engine)

    engine.connect.assert_not_called()


def test_init_runs_blob_check_on_read_and_write_engines(tmp_path, monkeypatch):
    """When read_path differs from path, the BLOB check runs on both engines."""
    checked_engines = []
    monkeypatch.setattr(
        SqlRegistry,
        "_warn_if_narrow_blob_columns",
        staticmethod(lambda engine, *args, **kwargs: checked_engines.append(engine)),
    )
    # Same sqlite file for both engines so create_all (write) and the cache
    # refresh (read) both see the registry tables, while remaining two distinct
    # Engine objects.
    db_url = f"sqlite:///{tmp_path / 'registry.db'}"
    config = SqlRegistryConfig(
        registry_type="sql",
        path=db_url,
        read_path=db_url,
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(config, "test_project", None)
    try:
        assert registry.read_engine is not registry.write_engine
        assert checked_engines == [registry.write_engine, registry.read_engine]
    finally:
        registry.teardown()


def test_warn_if_narrow_blob_columns_swallows_query_errors(caplog):
    """A failing diagnostic query must never propagate out of registry init."""
    engine = MagicMock()
    engine.dialect.name = "mysql"
    engine.connect.return_value.__enter__.side_effect = Exception("connection refused")

    with caplog.at_level(logging.ERROR):
        # Must not raise even though the query path blows up.
        SqlRegistry._warn_if_narrow_blob_columns(engine)

    # The failure is demoted to a debug note, never an error/exception.
    assert "still typed BLOB" not in caplog.text


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


def _serialize_udf_referencing_module(module_name: str) -> bytes:
    """Create a dill-serialized UDF that references a fake module.

    The function is defined inside a temporary module so that dill records
    a dependency on that module. After serialization, the module is removed
    from sys.modules so that deserializing the bytes will raise
    ModuleNotFoundError.
    """
    mod = types.ModuleType(module_name)
    mod.__package__ = module_name
    sys.modules[module_name] = mod
    exec("def _udf(x): return x", mod.__dict__)
    udf_bytes = dill.dumps(mod._udf)
    del sys.modules[module_name]
    return udf_bytes


def test_shared_registry_cross_project_udf_does_not_crash(shared_sqlite_db_path):
    """Initializing a SqlRegistry must not crash when another project in the
    same database has a feature view whose UDF references a module that is
    not installed in the current environment.

    Before the fix, proto() called from_proto() on every feature view across
    all projects, triggering dill.loads() which raised ModuleNotFoundError.
    """
    db_url = f"sqlite:///{shared_sqlite_db_path}"
    config = SqlRegistryConfig(
        registry_type="sql", path=db_url, purge_feast_metadata=False
    )

    registry_a = SqlRegistry(config, "project_a", None)

    entity = Entity(name="driver", join_keys=["driver_id"])
    registry_a.apply_entity(entity, "project_a")

    file_source = FileSource(
        path="driver_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    fv = FeatureView(
        name="driver_features",
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="conv_rate", dtype=Float32)],
        source=file_source,
    )
    registry_a.apply_feature_view(fv, "project_a")

    # Inject a UDF body that references a non-existent module directly into
    # the DB, simulating a feature view from another project that uses a
    # module not available in this environment.
    fake_udf_bytes = _serialize_udf_referencing_module("nonexistent_project_module")

    fv_proto = fv.to_proto()
    fv_proto.spec.project = "project_a"
    fv_proto.spec.feature_transformation.CopyFrom(
        FeatureTransformationV2(
            user_defined_function=UserDefinedFunctionV2(
                name="fake_udf",
                body=fake_udf_bytes,
                body_text="def _udf(x): return x",
                mode="python",
            )
        )
    )
    with registry_a.write_engine.begin() as conn:
        from sqlalchemy import update

        stmt = (
            update(feature_views)
            .where(
                feature_views.c.feature_view_name == "driver_features",
                feature_views.c.project_id == "project_a",
            )
            .values(feature_view_proto=fv_proto.SerializeToString())
        )
        conn.execute(stmt)

    # Creating a new SqlRegistry for project_b against the same DB should
    # NOT crash even though project_a has a UDF referencing an unavailable
    # module. proto() should read raw protos without deserializing UDFs.
    registry_b = SqlRegistry(config, "project_b", None)

    entity_b = Entity(name="customer", join_keys=["customer_id"])
    registry_b.apply_entity(entity_b, "project_b")
    retrieved = registry_b.get_entity("customer", "project_b")
    assert retrieved.name == "customer"

    # Verify project_a's data is still accessible in the cached proto
    proto = registry_b.proto()
    project_names = [p.spec.name for p in proto.projects]
    assert "project_a" in project_names
    assert "project_b" in project_names

    fv_names = [fv.spec.name for fv in proto.feature_views]
    assert "driver_features" in fv_names

    registry_a.teardown()


def test_list_all_feature_views_updated_since(sqlite_registry):
    """Test that _list_all_feature_views filters by updated_since at the SQL level."""
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

    fv1 = _build_feature_view("driver_activity_1", entity, file_source)
    fv2 = _build_feature_view("driver_activity_2", entity, file_source)
    sqlite_registry.apply_feature_view(fv1, "test_project")
    sqlite_registry.apply_feature_view(fv2, "test_project")

    # Filtering with a past timestamp returns all feature views
    past = datetime(2000, 1, 1, tzinfo=timezone.utc)
    result = sqlite_registry.list_all_feature_views("test_project", updated_since=past)
    assert len(result) == 2

    # Filtering with a future timestamp returns nothing
    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    result = sqlite_registry.list_all_feature_views(
        "test_project", updated_since=future
    )
    assert len(result) == 0

    # No filter returns all feature views
    result = sqlite_registry.list_all_feature_views("test_project")
    assert len(result) == 2


def test_list_feature_views_updated_since(sqlite_registry):
    """Test that _list_feature_views respects updated_since via SQL WHERE clause."""
    entity = Entity(
        name="rider",
        value_type=ValueType.STRING,
        join_keys=["rider_id"],
    )
    sqlite_registry.apply_entity(entity, "test_project")

    file_source = FileSource(
        path="rider_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )

    fv = _build_feature_view("rider_activity", entity, file_source)
    sqlite_registry.apply_feature_view(fv, "test_project")

    # A cutoff just before the feature view was applied returns it
    before = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
    result = sqlite_registry._list_feature_views(
        "test_project", tags=None, updated_since=before
    )
    assert any(fv.name == "rider_activity" for fv in result)

    # A cutoff in the future returns nothing
    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    result = sqlite_registry._list_feature_views(
        "test_project", tags=None, updated_since=future
    )
    assert len(result) == 0


def test_list_feature_views_updated_since_naive_treated_as_utc(sqlite_registry):
    """A naive updated_since is treated as UTC, not local time, in the SQL filter."""
    entity = Entity(
        name="courier",
        value_type=ValueType.STRING,
        join_keys=["courier_id"],
    )
    sqlite_registry.apply_entity(entity, "test_project")

    file_source = FileSource(
        path="courier_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )

    fv = _build_feature_view("courier_activity", entity, file_source)
    sqlite_registry.apply_feature_view(fv, "test_project")

    # A naive past cutoff (interpreted as UTC) should return the feature view
    past_naive = datetime(2000, 1, 1)
    result = sqlite_registry._list_feature_views(
        "test_project", tags=None, updated_since=past_naive
    )
    assert any(fv.name == "courier_activity" for fv in result)

    # The equivalent UTC-aware cutoff must produce the same result
    past_aware = datetime(2000, 1, 1, tzinfo=timezone.utc)
    result_aware = sqlite_registry._list_feature_views(
        "test_project", tags=None, updated_since=past_aware
    )
    assert len(result) == len(result_aware)
