import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.sqlite import (
    SqliteOfflineStore,
    SqliteOfflineStoreConfig,
    SqliteRetrievalJob,
    SqliteSource,
    _get_database_path,
    _get_entity_df_event_timestamp_range,
    _get_entity_schema,
)
from feast.types import from_value_type
from feast.value_type import ValueType


class DummyConfig:
    """Minimal config stub with offline_store and repo_path attributes."""

    def __init__(self, path: str, repo_path: Path = None):
        self.offline_store = SqliteOfflineStoreConfig(path=path)
        # repo_path is used to resolve relative paths
        self.repo_path = repo_path


def test_sqlitesource_validate_and_query():
    # Missing database raises
    src = SqliteSource(database=None, table="tbl")
    with pytest.raises(ValueError):
        src.validate(DummyConfig(path="db.sqlite"))
    # Missing table or query raises
    src = SqliteSource(database="db.sqlite", table=None, query=None)
    with pytest.raises(ValueError):
        src.validate(DummyConfig(path="db.sqlite"))
    # Table gives correct query
    src = SqliteSource(database="db.sqlite", table="tbl")
    assert src.get_table_query_string() == "SELECT * FROM tbl"
    # Custom query preserved
    src = SqliteSource(database="db.sqlite", query="SELECT x FROM tbl")
    assert src.get_table_query_string() == "SELECT x FROM tbl"


def test_get_database_path(tmp_path):
    # Absolute path unchanged
    abs_path = str(tmp_path / "abs.db")
    cfg = DummyConfig(path=abs_path, repo_path=tmp_path)
    assert _get_database_path(cfg) == abs_path
    # Relative path resolved against repo_path
    cfg = DummyConfig(path="rel.db", repo_path=tmp_path)
    expected = str(tmp_path / "rel.db")
    assert _get_database_path(cfg) == expected
    # No repo_path, return as-is
    cfg = DummyConfig(path="rel2.db", repo_path=None)
    assert _get_database_path(cfg) == "rel2.db"


def test_entity_schema_and_timestamp_range(tmp_path):
    # Prepare in-memory SQLite for SQL path
    conn = sqlite3.connect(":memory:")
    # Test DataFrame branch
    df = pd.DataFrame(
        {
            "a": [1, 2],
            "b": [3.0, 4.0],
            "ts": pd.to_datetime(["2020-01-01", "2020-02-01"], utc=True),
        }
    )
    schema = _get_entity_schema(df, conn)
    assert schema["a"].kind == "i"
    assert schema["b"].kind == "f"
    # Timestamp range from DataFrame
    min_ts, max_ts = _get_entity_df_event_timestamp_range(df, "ts", conn)
    assert min_ts == datetime(2020, 1, 1, tzinfo=min_ts.tzinfo)
    assert max_ts == datetime(2020, 2, 1, tzinfo=max_ts.tzinfo)
    # Test SQL string branch
    sql = "SELECT '2020-03-01' AS ts UNION ALL SELECT '2020-04-01' AS ts"
    schema2 = _get_entity_schema(sql, conn)
    # Types inferred as object (string)
    assert "ts" in schema2
    min_ts2, max_ts2 = _get_entity_df_event_timestamp_range(sql, "ts", conn)
    assert min_ts2 == datetime(2020, 3, 1, tzinfo=min_ts2.tzinfo)
    assert max_ts2 == datetime(2020, 4, 1, tzinfo=max_ts2.tzinfo)


def test_sqliteretrievaljob_to_df_and_arrow(tmp_path):
    # Create a temporary SQLite database with sample data
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE tbl (x INT, y TEXT)")
    conn.executemany("INSERT INTO tbl VALUES (?, ?)", [(1, "a"), (2, "b")])
    conn.commit()
    conn.close()
    # Test reading to DataFrame
    job = SqliteRetrievalJob(
        query="SELECT * FROM tbl", database_path=str(db_path), full_feature_names=False
    )
    df = job._to_df_internal()
    assert set(df.x.tolist()) == {1, 2}
    assert set(df.y.tolist()) == {"a", "b"}
    # Test reading to Arrow
    table = job._to_arrow_internal()
    assert isinstance(table, pa.Table)
    assert set(table.column("y").to_pylist()) == ["a", "b"] or set(
        table.column("y").to_pylist()
    ) == {"a", "b"}


def test_pull_latest_and_pull_all(tmp_path):
    # Setup DB with multiple rows per key
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE feat (key TEXT, val INT, ts TIMESTAMP)")
    rows = [
        ("k1", 10, "2020-01-01"),
        ("k1", 20, "2020-02-01"),
        ("k2", 99, "2020-01-15"),
    ]
    conn.executemany("INSERT INTO feat VALUES (?, ?, ?)", rows)
    conn.commit()
    conn.close()
    cfg = DummyConfig(path=str(db_path), repo_path=None)
    src = SqliteSource(database=str(db_path), table="feat", timestamp_field="ts")
    # pull_latest
    job_latest = SqliteOfflineStore.pull_latest_from_table_or_query(
        cfg,
        src,
        join_key_columns=["key"],
        feature_name_columns=["val"],
        timestamp_field="ts",
        created_timestamp_column=None,
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
    )
    df_latest = job_latest.to_df()
    # Expect latest per key
    assert set(df_latest.key.tolist()) == {"k1", "k2"}
    assert df_latest.loc[df_latest.key == "k1", "val"].iloc[0] == 20
    # pull_all
    job_all = SqliteOfflineStore.pull_all_from_table_or_query(
        cfg,
        src,
        join_key_columns=["key"],
        feature_name_columns=["val"],
        timestamp_field="ts",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 1, 31),
    )
    df_all = job_all.to_df()
    # Between Jan 1 and Jan 31 inclusive, two rows: one for k1 at Jan1, one for k2 at Jan15
    assert set(df_all.key.tolist()) == {"k1", "k2"}


def test_offline_write_batch(tmp_path):
    # Prepare empty DB
    db_path = tmp_path / "out.db"
    cfg = DummyConfig(path=str(db_path), repo_path=None)
    # Create a FeatureView with a SqliteSource
    table_name = "out_tbl"
    src = SqliteSource(database=str(db_path), table=table_name)
    fv = FeatureView(name="fv", source=src)
    # Create a PyArrow table
    pdf = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    arrow_table = pa.Table.from_pandas(pdf)
    # Write batch
    SqliteOfflineStore.offline_write_batch(cfg, fv, arrow_table, progress=None)
    # Verify data persisted
    conn = sqlite3.connect(str(db_path))
    cur = conn.execute(f"SELECT a, b FROM {table_name}")
    rows = cur.fetchall()
    # SQLite stores as TEXT, so numeric values are strings
    assert set(rows) == {("1", "x"), ("2", "y")}
    conn.close()


def test_get_historical_features_metadata(tmp_path):
    # Setup DB and feature table
    db_path = tmp_path / "hist.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE hist (key TEXT, ts TIMESTAMP, val TEXT)")
    conn.executemany(
        "INSERT INTO hist VALUES (?, ?, ?)",
        [
            ("k1", "2020-01-01 00:00:00+00:00", "old"),
            ("k1", "2020-01-02 00:00:00+00:00", "new"),
        ],
    )
    conn.commit()
    conn.close()
    # Entity DataFrame with event_timestamp column
    entity_df = pd.DataFrame(
        {
            "key": ["k1"],
            "event_timestamp": pd.to_datetime(["2020-01-02"], utc=True),
        }
    )
    # FeatureView setup
    src = SqliteSource(database=str(db_path), table="hist", timestamp_field="ts")
    entity = Entity(name="ent", join_keys=["key"], value_type=ValueType.STRING)
    # Schema: include key, ts, val
    fields = [
        Field(name="key", dtype=from_value_type(ValueType.STRING)),
        Field(name="ts", dtype=from_value_type(ValueType.STRING)),
        Field(name="val", dtype=from_value_type(ValueType.STRING)),
    ]
    fv = FeatureView(name="fv", source=src, schema=fields, entities=[entity])

    # Dummy registry returns no on-demand feature views
    class DummyRegistry:
        def list_on_demand_feature_views(self, project, allow_cache=False, tags=None):
            return []

    registry = DummyRegistry()
    # Invoke get_historical_features
    job = SqliteOfflineStore.get_historical_features(
        config=DummyConfig(path=str(db_path)),
        feature_views=[fv],
        feature_refs=["val"],
        entity_df=entity_df,
        registry=registry,
        project="proj",
        full_feature_names=True,
    )
    # Validate job and metadata
    assert isinstance(job, SqliteRetrievalJob)
    meta = job.metadata
    assert meta.features == ["val"]
    assert meta.keys == ["key"]
    # Timestamps match DataFrame
    assert (
        meta.min_event_timestamp == entity_df["event_timestamp"].min().to_pydatetime()
    )
    assert (
        meta.max_event_timestamp == entity_df["event_timestamp"].max().to_pydatetime()
    )
