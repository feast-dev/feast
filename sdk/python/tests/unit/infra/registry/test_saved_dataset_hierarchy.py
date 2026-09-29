# Copyright 2026 The Feast Authors
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
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, inspect, text

from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.registry.sql import (
    FeastRegistryHierarchySchemaError,
    SqlRegistry,
    SqlRegistryConfig,
)
from feast.infra.registry.sql import (
    metadata as registry_metadata,
)
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.saved_dataset import SavedDataset, SavedDatasetColumn


@pytest.fixture
def sqlite_registry():
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )
    registry = SqlRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


def _make_saved_dataset(
    name: str,
    namespace: str = "",
    collection: str = "",
    columns=None,
) -> SavedDataset:
    return SavedDataset(
        name=name,
        features=["fv:feature"],
        join_keys=["entity_id"],
        storage=SavedDatasetFileStorage(path="file:///tmp/dataset.parquet"),
        namespace=namespace,
        collection=collection,
        description="test dataset",
        columns=columns or [],
    )


def _create_legacy_saved_datasets_table(engine, *, with_row: bool = False):
    """Pre-hierarchy schema: no namespace/collection SQL columns."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE saved_datasets ("
                "saved_dataset_name VARCHAR(255) NOT NULL, "
                "project_id VARCHAR(255) NOT NULL, "
                "last_updated_timestamp BIGINT NOT NULL, "
                "saved_dataset_proto BLOB NOT NULL, "
                "PRIMARY KEY (saved_dataset_name, project_id))"
            )
        )
        if with_row:
            dataset = _make_saved_dataset(
                "claims", namespace="underwriting", collection="curated"
            )
            proto_bytes = dataset.to_proto().SerializeToString()
            conn.execute(
                text(
                    "INSERT INTO saved_datasets "
                    "(saved_dataset_name, project_id, last_updated_timestamp, "
                    "saved_dataset_proto) VALUES (:name, :project, :ts, :proto)"
                ),
                {
                    "name": "claims",
                    "project": "test_project",
                    "ts": 1,
                    "proto": proto_bytes,
                },
            )


def test_saved_dataset_columns_round_trip(sqlite_registry):
    dataset = _make_saved_dataset(
        name="claims",
        namespace="underwriting",
        collection="curated",
        columns=[
            SavedDatasetColumn(name="claim_id", type="string", description="id"),
            SavedDatasetColumn(name="amount", type="double", nullable=False),
        ],
    )

    sqlite_registry.apply_saved_dataset(dataset, project="test_project")
    loaded = sqlite_registry.get_saved_dataset("claims", project="test_project")

    assert loaded.namespace == "underwriting"
    assert loaded.collection == "curated"
    assert loaded.description == "test dataset"
    assert loaded.columns == [
        SavedDatasetColumn(name="claim_id", type="string", description="id"),
        SavedDatasetColumn(name="amount", type="double", nullable=False),
    ]
    assert loaded.columns[0].nullable is True  # OpenAPI / SDK default
    assert loaded.columns[1].nullable is False


def test_list_saved_datasets_filters_by_namespace_sql(sqlite_registry):
    sqlite_registry.apply_saved_dataset(
        _make_saved_dataset("a", namespace="ns1"), project="test_project"
    )
    sqlite_registry.apply_saved_dataset(
        _make_saved_dataset("b", namespace="ns2"), project="test_project"
    )
    sqlite_registry.apply_saved_dataset(
        _make_saved_dataset("c", namespace="ns1", collection="raw"),
        project="test_project",
    )

    ns1 = sqlite_registry.list_saved_datasets(project="test_project", namespace="ns1")
    assert sorted(d.name for d in ns1) == ["a", "c"]

    ns1_raw = sqlite_registry.list_saved_datasets(
        project="test_project", namespace="ns1", collection="raw"
    )
    assert [d.name for d in ns1_raw] == ["c"]


def test_saved_datasets_sql_has_hierarchy_columns_and_index(sqlite_registry):
    engine = sqlite_registry.write_engine
    inspector = inspect(engine)
    column_names = {
        column["name"] for column in inspector.get_columns("saved_datasets")
    }
    assert "namespace" in column_names
    assert "collection" in column_names

    index_names = {index["name"] for index in inspector.get_indexes("saved_datasets")}
    assert "idx_saved_datasets_project_namespace" in index_names


def test_ensure_hierarchy_columns_migrates_legacy_table():
    """Existing DBs without hierarchy columns get them via additive migration."""
    fd, registry_path = tempfile.mkstemp()
    engine = create_engine(f"sqlite:///{registry_path}")
    _create_legacy_saved_datasets_table(engine)

    SqlRegistry._ensure_saved_dataset_hierarchy_columns(engine)

    inspector = inspect(engine)
    column_names = {
        column["name"] for column in inspector.get_columns("saved_datasets")
    }
    assert "namespace" in column_names
    assert "collection" in column_names
    index_names = {index["name"] for index in inspector.get_indexes("saved_datasets")}
    assert "idx_saved_datasets_project_namespace" in index_names

    # Second call is a no-op and must not fail.
    SqlRegistry._ensure_saved_dataset_hierarchy_columns(engine)


def test_ensure_backfills_hierarchy_from_proto():
    """Migration must copy namespace/collection out of the proto blob into SQL."""
    fd, registry_path = tempfile.mkstemp()
    engine = create_engine(f"sqlite:///{registry_path}")
    _create_legacy_saved_datasets_table(engine, with_row=True)

    SqlRegistry._ensure_saved_dataset_hierarchy_columns(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT namespace, collection, saved_dataset_proto "
                "FROM saved_datasets WHERE saved_dataset_name = 'claims'"
            )
        ).one()

    proto = SavedDatasetProto.FromString(row.saved_dataset_proto)
    assert row.namespace == proto.spec.namespace == "underwriting"
    assert row.collection == proto.spec.collection == "curated"

    # Filter would have missed this row if SQL stayed at default ''.
    assert row.namespace == "underwriting"


def test_backfill_skips_rows_with_non_empty_hierarchy():
    """Steady-state backfill must not rewrite rows that already have SQL values."""
    fd, registry_path = tempfile.mkstemp()
    engine = create_engine(f"sqlite:///{registry_path}")
    _create_legacy_saved_datasets_table(engine, with_row=True)
    SqlRegistry._ensure_saved_dataset_hierarchy_columns(engine)

    # Simulate a non-empty (even wrong) SQL value — backfill must leave it alone.
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE saved_datasets SET namespace = 'stale-ns', "
                "collection = 'stale-coll' WHERE saved_dataset_name = 'claims'"
            )
        )

    SqlRegistry._backfill_saved_dataset_hierarchy_columns(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT namespace, collection FROM saved_datasets "
                "WHERE saved_dataset_name = 'claims'"
            )
        ).one()
    assert row.namespace == "stale-ns"
    assert row.collection == "stale-coll"


def test_saved_dataset_column_proto_round_trip():
    column = SavedDatasetColumn(
        name="x", type="long", description="desc", nullable=False
    )
    restored = SavedDatasetColumn.from_proto(column.to_proto())
    assert restored == column
    assert restored.nullable is False

    defaulted = SavedDatasetColumn(name="y", type="string")
    assert defaulted.nullable is True
    assert SavedDatasetColumn.from_proto(defaulted.to_proto()).nullable is True


def test_saved_dataset_column_nullable_absent_defaults_true():
    """Unset optional proto bool must not become False (OpenAPI default true)."""
    from feast.protos.feast.core.SavedDataset_pb2 import (
        SavedDatasetColumn as SavedDatasetColumnProto,
    )

    unset = SavedDatasetColumnProto(name="x", type="string")
    assert not unset.HasField("nullable")
    assert SavedDatasetColumn.from_proto(unset).nullable is True

    explicit_false = SavedDatasetColumnProto(name="y", type="string", nullable=False)
    assert explicit_false.HasField("nullable")
    assert SavedDatasetColumn.from_proto(explicit_false).nullable is False


def test_sql_hierarchy_columns_match_proto_after_apply(sqlite_registry):
    """Denormalized SQL columns must stay in sync with proto (source of truth)."""
    dataset = _make_saved_dataset(
        name="claims",
        namespace="underwriting",
        collection="curated",
    )
    sqlite_registry.apply_saved_dataset(dataset, project="test_project")

    with sqlite_registry.write_engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT namespace, collection, saved_dataset_proto "
                "FROM saved_datasets "
                "WHERE saved_dataset_name = :name AND project_id = :project"
            ),
            {"name": "claims", "project": "test_project"},
        ).one()

    loaded = SavedDataset.from_proto(
        SavedDatasetProto.FromString(row.saved_dataset_proto)
    )
    assert row.namespace == loaded.namespace == "underwriting"
    assert row.collection == loaded.collection == "curated"


def test_reapply_heals_stale_or_empty_hierarchy_sql_columns(sqlite_registry):
    """Proto-equal re-apply must still repair drifted denormalized SQL columns.

    After additive migration (or a buggy writer), SQL namespace/collection can be
    empty or stale while the proto already has the correct hierarchy. Equality
    short-circuit must not skip healing those columns.
    """
    dataset = _make_saved_dataset(
        name="claims",
        namespace="underwriting",
        collection="curated",
    )
    sqlite_registry.apply_saved_dataset(dataset, project="test_project")

    with sqlite_registry.write_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE saved_datasets SET namespace = '', collection = 'stale' "
                "WHERE saved_dataset_name = :name AND project_id = :project"
            ),
            {"name": "claims", "project": "test_project"},
        )

    # Same object (proto equality) — must still rewrite denorm columns.
    sqlite_registry.apply_saved_dataset(dataset, project="test_project")

    with sqlite_registry.write_engine.begin() as conn:
        row = conn.execute(
            text(
                "SELECT namespace, collection FROM saved_datasets "
                "WHERE saved_dataset_name = :name AND project_id = :project"
            ),
            {"name": "claims", "project": "test_project"},
        ).one()

    assert row.namespace == "underwriting"
    assert row.collection == "curated"
    listed = sqlite_registry.list_saved_datasets(
        project="test_project", namespace="underwriting", collection="curated"
    )
    assert [ds.name for ds in listed] == ["claims"]


def test_require_hierarchy_raises_on_legacy_schema():
    fd, registry_path = tempfile.mkstemp()
    engine = create_engine(f"sqlite:///{registry_path}")
    _create_legacy_saved_datasets_table(engine)

    with pytest.raises(FeastRegistryHierarchySchemaError, match="hierarchy schema"):
        SqlRegistry._require_saved_dataset_hierarchy_columns(
            engine, engine_role="read engine"
        )


def test_require_does_not_alter_schema():
    """Fail-fast path must never ALTER TABLE."""
    fd, registry_path = tempfile.mkstemp()
    engine = create_engine(f"sqlite:///{registry_path}")
    _create_legacy_saved_datasets_table(engine)

    with pytest.raises(FeastRegistryHierarchySchemaError):
        SqlRegistry._require_saved_dataset_hierarchy_columns(engine)

    inspector = inspect(engine)
    column_names = {
        column["name"] for column in inspector.get_columns("saved_datasets")
    }
    assert "namespace" not in column_names
    assert "collection" not in column_names


def test_schema_mode_verify_does_not_run_ensure(tmp_path):
    db_file = tmp_path / "verify_hierarchy.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    registry_metadata.create_all(engine)
    engine.dispose()

    with patch.object(
        SqlRegistry, "_ensure_saved_dataset_hierarchy_columns"
    ) as mock_ensure:
        config = SqlRegistryConfig(
            registry_type="sql",
            path=db_url,
            schema_mode="verify",
        )
        registry = SqlRegistry(config, "test_project", None)
        mock_ensure.assert_not_called()
        registry.teardown()


def test_schema_mode_skip_does_not_run_ensure(tmp_path):
    db_file = tmp_path / "skip_hierarchy.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    registry_metadata.create_all(engine)
    engine.dispose()

    with patch.object(
        SqlRegistry, "_ensure_saved_dataset_hierarchy_columns"
    ) as mock_ensure:
        config = SqlRegistryConfig(
            registry_type="sql",
            path=db_url,
            schema_mode="skip",
        )
        registry = SqlRegistry(config, "test_project", None)
        mock_ensure.assert_not_called()
        registry.teardown()


def test_schema_mode_verify_raises_when_hierarchy_missing(tmp_path, monkeypatch):
    """verify must not ALTER; missing hierarchy fails startup (any catalog mode)."""
    monkeypatch.delenv("DATACATALOG_ENABLED", raising=False)
    db_file = tmp_path / "verify_legacy.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    registry_metadata.create_all(engine)
    # Simulate a pre-hierarchy saved_datasets table.
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE saved_datasets"))
    _create_legacy_saved_datasets_table(engine)
    engine.dispose()

    config = SqlRegistryConfig(
        registry_type="sql",
        path=db_url,
        schema_mode="verify",
    )
    with pytest.raises(FeastRegistryHierarchySchemaError, match="write engine"):
        SqlRegistry(config, "test_project", None)


def test_schema_mode_verify_legacy_raises_with_catalog(tmp_path, monkeypatch):
    """Catalog + verify: same hard fail when hierarchy columns are missing."""
    monkeypatch.setenv("DATACATALOG_ENABLED", "true")
    db_file = tmp_path / "verify_legacy_catalog.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    registry_metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE saved_datasets"))
    _create_legacy_saved_datasets_table(engine)
    engine.dispose()

    config = SqlRegistryConfig(
        registry_type="sql",
        path=db_url,
        schema_mode="verify",
    )
    with pytest.raises(FeastRegistryHierarchySchemaError, match="hierarchy schema"):
        SqlRegistry(config, "test_project", None)


def test_read_replica_missing_hierarchy_fails_startup(tmp_path, monkeypatch):
    """verify: lagging read schema must fail init (catalog or not)."""
    monkeypatch.delenv("DATACATALOG_ENABLED", raising=False)
    write_file = tmp_path / "write.db"
    read_file = tmp_path / "read.db"
    write_url = f"sqlite:///{write_file}"
    read_url = f"sqlite:///{read_file}"

    write_engine = create_engine(write_url)
    registry_metadata.create_all(write_engine)
    write_engine.dispose()

    read_engine = create_engine(read_url)
    registry_metadata.create_all(read_engine)
    with read_engine.begin() as conn:
        conn.execute(text("DROP TABLE saved_datasets"))
    _create_legacy_saved_datasets_table(read_engine)
    read_engine.dispose()

    config = SqlRegistryConfig(
        registry_type="sql",
        path=write_url,
        read_path=read_url,
        schema_mode="verify",
    )
    with pytest.raises(FeastRegistryHierarchySchemaError, match="read engine"):
        SqlRegistry(config, "test_project", None)


def test_read_replica_missing_hierarchy_fails_with_catalog(tmp_path, monkeypatch):
    """Catalog + verify: lagging read schema fails with read-engine message."""
    monkeypatch.setenv("DATACATALOG_ENABLED", "true")
    write_file = tmp_path / "write_c.db"
    read_file = tmp_path / "read_c.db"
    write_url = f"sqlite:///{write_file}"
    read_url = f"sqlite:///{read_file}"

    write_engine = create_engine(write_url)
    registry_metadata.create_all(write_engine)
    write_engine.dispose()

    read_engine = create_engine(read_url)
    registry_metadata.create_all(read_engine)
    with read_engine.begin() as conn:
        conn.execute(text("DROP TABLE saved_datasets"))
    _create_legacy_saved_datasets_table(read_engine)
    read_engine.dispose()

    config = SqlRegistryConfig(
        registry_type="sql",
        path=write_url,
        read_path=read_url,
        schema_mode="verify",
    )
    with pytest.raises(FeastRegistryHierarchySchemaError, match="read engine"):
        SqlRegistry(config, "test_project", None)
