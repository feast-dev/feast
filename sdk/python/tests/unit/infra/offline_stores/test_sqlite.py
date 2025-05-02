import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

from feast.infra.offline_stores.sqlite import (
    SqliteOfflineStore,
    SqliteOfflineStoreConfig,
    SqliteRetrievalJob,
    SqliteSource,
)
from feast.repo_config import RepoConfig


def test_sqlite_retrieval_job():
    """Test that SqliteRetrievalJob correctly executes a query and returns results."""
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_db:
        import sqlite3

        conn = sqlite3.connect(temp_db.name)
        df = pd.DataFrame(
            {
                "driver_id": [1, 2, 3],
                "event_timestamp": [
                    datetime.now(),
                    datetime.now() - timedelta(hours=1),
                    datetime.now() - timedelta(hours=2),
                ],
                "conv_rate": [0.5, 0.3, 0.4],
            }
        )
        df.to_sql("drivers", conn, index=False)

        query = "SELECT * FROM drivers ORDER BY event_timestamp DESC"
        job = SqliteRetrievalJob(
            query=query, database_path=temp_db.name, full_feature_names=False
        )

        result_df = job._to_df_internal()
        assert len(result_df) == 3
        assert "driver_id" in result_df.columns
        assert "conv_rate" in result_df.columns

        result_arrow = job._to_arrow_internal()
        assert isinstance(result_arrow, pa.Table)
        assert result_arrow.num_rows == 3
        assert "driver_id" in result_arrow.column_names
        assert "conv_rate" in result_arrow.column_names


def test_sqlite_source_validation():
    """Test that SqliteSource correctly validates its configuration."""
    source = SqliteSource(database="test.db", table="drivers")
    config = MagicMock()
    source.validate(config)

    source = SqliteSource(table="drivers")
    with pytest.raises(ValueError, match="database is required"):
        source.validate(config)

    source = SqliteSource(database="test.db")
    with pytest.raises(ValueError, match="Either query or table must be specified"):
        source.validate(config)


def test_sqlite_offline_store_config():
    """Test that SqliteOfflineStoreConfig correctly handles paths."""
    config = SqliteOfflineStoreConfig()
    assert config.path == "data/offline.db"

    config = SqliteOfflineStoreConfig(path="/custom/path/to/db.sqlite")
    assert config.path == "/custom/path/to/db.sqlite"


def test_get_historical_features():
    """Test that SqliteOfflineStore.get_historical_features works correctly."""
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_db:
        config = RepoConfig(
            repo_path=".", offline_store=SqliteOfflineStoreConfig(path=temp_db.name)
        )

        import sqlite3

        conn = sqlite3.connect(temp_db.name)

        driver_df = pd.DataFrame(
            {
                "driver_id": [1, 2, 3],
                "event_timestamp": [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
                    (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
                ],
                "conv_rate": [0.5, 0.3, 0.4],
                "acc_rate": [0.9, 0.8, 0.7],
            }
        )
        driver_df.to_sql("driver_stats", conn, index=False)

        entity_df = pd.DataFrame(
            {
                "driver_id": [1, 2],
                "event_timestamp": [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
                ],
            }
        )
        entity_df.to_sql("entity_df", conn, index=False)

        feature_views = [MagicMock()]
        feature_views[0].batch_source = SqliteSource(
            database=temp_db.name, table="driver_stats"
        )
        feature_views[0].entities = ["driver_id"]
        feature_views[0].features = ["conv_rate", "acc_rate"]

        registry = MagicMock()
        registry.list_on_demand_feature_views.return_value = []

        with (
            patch(
                "feast.infra.offline_stores.offline_utils.get_feature_view_query_context"
            ),
            patch(
                "feast.infra.offline_stores.offline_utils.build_point_in_time_query",
                return_value="SELECT * FROM entity_df",
            ),
        ):
            retrieval_job = SqliteOfflineStore.get_historical_features(
                config=config,
                feature_views=feature_views,
                feature_refs=["driver_stats:conv_rate", "driver_stats:acc_rate"],
                entity_df=entity_df,
                registry=registry,
                project="default",
                full_feature_names=False,
            )

            assert isinstance(retrieval_job, SqliteRetrievalJob)
            result_df = retrieval_job.to_df()
            assert isinstance(result_df, pd.DataFrame)


def test_offline_write_batch():
    """Test that SqliteOfflineStore.offline_write_batch works correctly."""
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_db:
        config = RepoConfig(
            repo_path=".", offline_store=SqliteOfflineStoreConfig(path=temp_db.name)
        )

        feature_view = MagicMock()
        feature_view.batch_source = SqliteSource(
            database=temp_db.name, table="driver_stats"
        )

        df = pd.DataFrame(
            {
                "driver_id": [1, 2, 3],
                "event_timestamp": [
                    datetime.now(),
                    datetime.now() - timedelta(hours=1),
                    datetime.now() - timedelta(hours=2),
                ],
                "conv_rate": [0.5, 0.3, 0.4],
            }
        )
        table = pa.Table.from_pandas(df)

        progress_callback = MagicMock()

        SqliteOfflineStore.offline_write_batch(
            config=config,
            feature_view=feature_view,
            table=table,
            progress=progress_callback,
        )

        import sqlite3

        conn = sqlite3.connect(temp_db.name)
        result_df = pd.read_sql_query("SELECT * FROM driver_stats", conn)
        assert len(result_df) == 3
        assert "driver_id" in result_df.columns
        assert "conv_rate" in result_df.columns

        progress_callback.assert_called_once_with(3)
