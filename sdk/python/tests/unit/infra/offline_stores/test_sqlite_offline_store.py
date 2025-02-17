import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from feast import RepoConfig
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.sqlite import SQLiteOfflineStore, SQLiteRetrievalJob
from feast.infra.offline_stores.sqlite_config import SQLiteOfflineStoreConfig
from feast.infra.offline_stores.sqlite_source import SQLiteSource
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.types import Float32, Int64, ValueType


def test_sqlite_offline_store_config():
    config = SQLiteOfflineStoreConfig(type="sqlite", path=":memory:")
    assert config.type == "sqlite"
    assert config.path == ":memory:"


def test_sqlite_offline_store_config_with_timeout():
    config = SQLiteOfflineStoreConfig(
        type="sqlite", path=":memory:", connection_timeout=5.0
    )
    assert config.connection_timeout == 5.0


def test_sqlite_offline_store_with_temp_file():
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_db:
        config = RepoConfig(
            provider="local",
            project="test",
            registry="memory://",
            offline_store=SQLiteOfflineStoreConfig(type="sqlite", path=temp_db.name),
        )
        store = SQLiteOfflineStore()
        entity = Entity(
            name="driver", join_keys=["driver_id"], value_type=ValueType.INT64
        )
        feature_view = FeatureView(
            name="driver_stats",
            entities=[entity],
            schema=[
                Field(name="driver_id", dtype=Int64),
                Field(name="avg_speed", dtype=Float32),
            ],
            source=SQLiteSource(
                table="driver_stats",
                timestamp_field="event_timestamp",
            ),
        )

        entity_df = pd.DataFrame(
            {
                "driver_id": [1, 2],
                "event_timestamp": [
                    datetime.now() - timedelta(days=1),
                    datetime.now(),
                ],
            }
        )

        job = store.get_historical_features(
            config=config,
            feature_views=[feature_view],
            feature_refs=["driver_stats:avg_speed"],
            entity_df=entity_df,
            registry=Registry(
                registry_config=RegistryConfig(
                    registry_store_type="FileRegistryStore",
                    path=str(config.registry),
                    cache_ttl_seconds=600,
                ),
                project=config.project,
                repo_path=Path("."),
            ),
            project=config.project,
        )
        assert job.__class__.__name__ == "SQLiteRetrievalJob"
        assert job.full_feature_names is False
        assert len(job.on_demand_feature_views) == 0


def test_sqlite_source_validation():
    with pytest.raises(
        ValueError, match="SQLite source must have either table or query specified"
    ):
        source = SQLiteSource()
        source.validate(
            RepoConfig(
                provider="local",
                project="test",
                registry="memory://",
                offline_store=SQLiteOfflineStoreConfig(type="sqlite", path=":memory:"),
            )
        )


def test_sqlite_source_table_query():
    source = SQLiteSource(table="my_table")
    assert source.get_table_query_string() == "my_table"

    source = SQLiteSource(query="SELECT * FROM my_table")
    assert source.get_table_query_string() == "(SELECT * FROM my_table)"


def test_sqlite_retrieval_job_to_sql():
    config = SQLiteOfflineStoreConfig(type="sqlite", path=":memory:")
    job = SQLiteRetrievalJob(
        query="SELECT * FROM test",
        config=config,
        full_feature_names=False,
    )
    assert job.to_sql() == "SELECT * FROM test"


def test_sqlite_retrieval_job_to_arrow():
    config = SQLiteOfflineStoreConfig(type="sqlite", path=":memory:")
    job = SQLiteRetrievalJob(
        query="SELECT * FROM test",
        config=config,
        full_feature_names=False,
    )
    with pytest.raises(Exception):
        job.to_arrow()  # Should fail since table doesn't exist


def test_sqlite_retrieval_job_persist():
    config = SQLiteOfflineStoreConfig(type="sqlite", path=":memory:")
    job = SQLiteRetrievalJob(
        query="SELECT * FROM test",
        config=config,
        full_feature_names=False,
    )
    with pytest.raises(Exception):
        job.persist(SavedDatasetStorage())


def test_sqlite_source_get_table_column_names_and_types():
    with tempfile.NamedTemporaryFile(suffix=".json") as registry_file:
        config = RepoConfig(
            registry=registry_file.name,
            project="test",
            provider="local",
            offline_store=SQLiteOfflineStoreConfig(type="sqlite", path=":memory:"),
        )
        # Create test table in the same connection that will be used by get_table_column_names_and_types
        store = SQLiteOfflineStore()
        conn = sqlite3.connect(":memory:")
        setattr(store, "_conn", conn)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER,
                value REAL,
                text_col TEXT,
                timestamp DATETIME,
                bool_col BOOLEAN,
                blob_col BLOB,
                numeric_col NUMERIC,
                date_col DATE,
                time_col TIME,
                varchar_col VARCHAR(255),
                char_col CHAR(10)
            )
        """)
        conn.commit()
        # Use the same connection for getting column types
        source = SQLiteSource(table="test_table")
        if isinstance(config.offline_store, SQLiteOfflineStoreConfig):
            setattr(config.offline_store, "_conn", conn)
            columns = source.get_table_column_names_and_types(config)
    assert len(columns) == 11
    assert ("id", "INTEGER") in columns
    assert ("value", "REAL") in columns
    assert ("text_col", "TEXT") in columns
    assert ("timestamp", "DATETIME") in columns
    assert ("bool_col", "BOOLEAN") in columns
    assert ("blob_col", "BLOB") in columns
    assert ("numeric_col", "NUMERIC") in columns
    assert ("date_col", "DATE") in columns
    assert ("time_col", "TIME") in columns
    assert ("varchar_col", "VARCHAR") in columns
    assert ("char_col", "CHAR") in columns


def test_sqlite_source_invalid_config():
    with tempfile.NamedTemporaryFile(suffix=".json") as registry_file:
        with pytest.raises(ValueError, match="Input should be 'sqlite'"):
            RepoConfig(
                registry=registry_file.name,
                project="test",
                provider="local",
                offline_store=SQLiteOfflineStoreConfig(type="invalid"),
            )


def test_sqlite_source_invalid_table():
    with tempfile.NamedTemporaryFile(suffix=".json") as registry_file:
        source = SQLiteSource(table="nonexistent_table")
        config = RepoConfig(
            registry=registry_file.name,
            project="test",
            provider="local",
            offline_store=SQLiteOfflineStoreConfig(type="sqlite", path=":memory:"),
        )
    with pytest.raises(Exception):
        source.get_table_column_names_and_types(config)


def test_sqlite_source_invalid_query():
    with tempfile.NamedTemporaryFile(suffix=".json") as registry_file:
        source = SQLiteSource(query="SELECT * FROM nonexistent_table")
        config = RepoConfig(
            registry=registry_file.name,
            project="test",
            provider="local",
            offline_store=SQLiteOfflineStoreConfig(type="sqlite", path=":memory:"),
        )
    with pytest.raises(Exception):
        source.get_table_column_names_and_types(config)


def test_sqlite_data_type_mapping():
    with tempfile.NamedTemporaryFile(suffix=".json") as registry_file:
        store = SQLiteOfflineStore()
        conn = sqlite3.connect(":memory:")
        setattr(store, "_conn", conn)
        offline_store_config = SQLiteOfflineStoreConfig(type="sqlite", path=":memory:")
        setattr(offline_store_config, "_conn", conn)
        config = RepoConfig(
            registry=registry_file.name,
            project="test",
            provider="local",
            offline_store=offline_store_config,
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE test_types (
                int_col INTEGER,
                real_col REAL,
                text_col TEXT,
                blob_col BLOB,
                bool_col BOOLEAN,
                date_col DATE,
                time_col TIME,
                datetime_col DATETIME,
                numeric_col NUMERIC
            )
        """)
        cursor.execute("""
            INSERT INTO test_types VALUES (
                1,
                3.14,
                'text',
                X'deadbeef',
                1,
                '2024-01-01',
                '12:34:56',
                '2024-01-01 12:34:56',
                123.45
            )
        """)
        conn.commit()
        source = SQLiteSource(table="test_types", timestamp_field="datetime_col")
        setattr(source, "_conn", conn)
        result = store.pull_latest_from_table_or_query(
            config=config,
            data_source=source,
            join_key_columns=["int_col"],
            feature_name_columns=["real_col", "text_col", "bool_col", "numeric_col"],
            timestamp_field="datetime_col",
            created_timestamp_column="datetime_col",
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
        )
        df = result.to_df()
        assert df["int_col"].dtype == "Int64"  # Using pandas nullable integer type
        assert df["real_col"].dtype == "float64"
        assert df["text_col"].dtype == "object"
        assert df["bool_col"].dtype == "bool"
        assert df["numeric_col"].dtype == "float64"
        conn.close()


def test_sqlite_connection_timeout():
    config = SQLiteOfflineStoreConfig(
        type="sqlite",
        path=":memory:",
        connection_timeout=0.001,  # Very short timeout
    )
    store = SQLiteOfflineStore()
    # Create a connection that holds a lock
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("BEGIN EXCLUSIVE")
    with pytest.raises(Exception):
        store.pull_latest_from_table_or_query(
            config=config,
            data_source=SQLiteSource(table="test"),
            join_key_columns=["id"],
            feature_name_columns=["feature"],
            timestamp_field="ts",
            created_timestamp_column="created_ts",
            start_date=datetime.now(),
            end_date=datetime.now(),
        )
    conn.close()
