import sqlite3
import tempfile
from datetime import datetime

import pandas as pd

from feast.infra.offline_stores.contrib.sqlite_offline_store import (
    SQLiteOfflineStore,
    SQLiteSource,
)
from feast.infra.offline_stores.contrib.sqlite_repo_configuration import (
    SQLiteOfflineStoreConfig,
)
from feast.repo_config import RepoConfig


def test_sqlite_source_creation():
    """Test basic SQLite source creation and properties."""
    source = SQLiteSource(
        database="test.db", table="test_table", timestamp_field="event_timestamp"
    )

    assert source.database == "test.db"
    assert source.table == "test_table"
    assert source.timestamp_field == "event_timestamp"
    assert source.path == "sqlite://test.db?table=test_table"


def test_sqlite_source_with_query():
    """Test SQLite source creation with a query."""
    source = SQLiteSource(
        database="test.db",
        table="test_table",
        query="SELECT * FROM test_table WHERE active = 1",
        timestamp_field="event_timestamp",
    )

    assert source.query == "SELECT * FROM test_table WHERE active = 1"
    assert "query=" in source.path


def test_sqlite_source_proto_conversion():
    """Test SQLite source protobuf serialization and deserialization."""
    original_source = SQLiteSource(
        database="test.db",
        table="test_table",
        timestamp_field="event_timestamp",
        name="test_source",
    )

    # Convert to proto and back
    proto = original_source.to_proto()
    restored_source = SQLiteSource.from_proto(proto)

    assert restored_source.database == original_source.database
    assert restored_source.table == original_source.table
    assert restored_source.timestamp_field == original_source.timestamp_field
    assert restored_source.name == original_source.name


def test_sqlite_offline_store_basic():
    """Test basic SQLite offline store functionality."""
    # Create a temporary SQLite database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp_db:
        db_path = temp_db.name

    # Create test data
    data = {
        "entity_id": [1, 2, 3],
        "feature_1": [10, 20, 30],
        "event_timestamp": [
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 12, 0, 0),
        ],
    }
    df = pd.DataFrame(data)

    # Write to SQLite database
    with sqlite3.connect(db_path) as conn:
        df.to_sql("features", conn, if_exists="replace", index=False)

    # Create SQLite source
    source = SQLiteSource(
        database=db_path, table="features", timestamp_field="event_timestamp"
    )

    # Test pull_all_from_table_or_query
    config = RepoConfig(
        project="test",
        registry="local",
        provider="local",
        offline_store=SQLiteOfflineStoreConfig(),
    )

    job = SQLiteOfflineStore.pull_all_from_table_or_query(
        config=config,
        data_source=source,
        join_key_columns=["entity_id"],
        feature_name_columns=["feature_1"],
        timestamp_field="event_timestamp",
    )

    result_df = job.to_df()
    assert len(result_df) == 3
    assert "entity_id" in result_df.columns
    assert "feature_1" in result_df.columns
    assert "event_timestamp" in result_df.columns


if __name__ == "__main__":
    test_sqlite_source_creation()
    test_sqlite_source_with_query()
    test_sqlite_source_proto_conversion()
    test_sqlite_offline_store_basic()
    print("All tests passed!")
