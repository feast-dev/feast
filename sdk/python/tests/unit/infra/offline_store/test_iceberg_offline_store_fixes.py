"""Unit tests for critical bug fixes in Iceberg Offline Store.

Tests cover:
1. TTL filtering enforcement in ASOF joins
2. SQL injection prevention
3. Deterministic tie-breaking with created_timestamp
"""

import pandas as pd
import pytest
from datetime import datetime, timedelta


pyiceberg = pytest.importorskip("pyiceberg")
duckdb = pytest.importorskip("duckdb")


from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
    IcebergOfflineStore,
    IcebergOfflineStoreConfig,
)


def test_sql_injection_prevention_rejects_sql_strings():
    """Test that SQL string input is rejected to prevent SQL injection."""
    from feast.repo_config import RepoConfig

    config = RepoConfig(
        project="test_project",
        registry="registry.db",
        provider="local",
        offline_store=IcebergOfflineStoreConfig(
            catalog_type="sql",
            uri="sqlite:///test.db",
        ),
    )

    # Attempt SQL injection via entity_df
    malicious_sql = "SELECT * FROM features; DROP TABLE features; --"

    with pytest.raises(ValueError, match="must be a pandas DataFrame"):
        IcebergOfflineStore.get_historical_features(
            config=config,
            feature_views=[],
            feature_refs=[],
            entity_df=malicious_sql,  # SQL string instead of DataFrame
            registry=None,
            project="test_project",
        )


def test_sql_injection_prevention_accepts_dataframes():
    """Test that valid DataFrame input is accepted."""
    from feast.repo_config import RepoConfig
    from unittest.mock import MagicMock, patch

    config = RepoConfig(
        project="test_project",
        registry="registry.db",
        provider="local",
        offline_store=IcebergOfflineStoreConfig(
            catalog_type="sql",
            uri="sqlite:///test.db",
        ),
    )

    # Valid DataFrame input
    entity_df = pd.DataFrame({
        "driver_id": [1, 2, 3],
        "event_timestamp": [datetime.now()] * 3,
    })

    # Mock the catalog and DuckDB operations
    with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog") as mock_catalog:
        mock_catalog.return_value = MagicMock()

        # This should NOT raise an error
        try:
            result = IcebergOfflineStore.get_historical_features(
                config=config,
                feature_views=[],
                feature_refs=[],
                entity_df=entity_df,
                registry=MagicMock(),
                project="test_project",
            )
            # Expected to work (though may fail later due to missing mocks)
        except ValueError as e:
            if "must be a pandas DataFrame" in str(e):
                pytest.fail("Should accept DataFrame input")
            # Other errors are acceptable in this unit test
        except Exception:
            # Other exceptions are fine - we're only testing SQL injection prevention
            pass


def test_ttl_filter_query_construction():
    """Test that TTL filter is correctly added to ASOF JOIN query."""
    from feast.feature_view import FeatureView
    from feast.field import Field
    from feast.types import Int32
    from feast.entity import Entity
    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import IcebergSource
    from feast.repo_config import RepoConfig
    from unittest.mock import MagicMock, patch
    import duckdb

    # Create entity
    driver_entity = Entity(name="driver", join_keys=["driver_id"])

    # Create a feature view with TTL
    source = IcebergSource(
        name="test_source",
        table_identifier="test.features",
        timestamp_field="event_timestamp",
    )

    feature_view = FeatureView(
        name="test_fv",
        entities=[driver_entity],
        schema=[Field(name="feature1", dtype=Int32)],
        source=source,
        ttl=timedelta(hours=24),  # 24-hour TTL
    )

    config = RepoConfig(
        project="test_project",
        registry="registry.db",
        provider="local",
        offline_store=IcebergOfflineStoreConfig(
            catalog_type="sql",
            uri="sqlite:///test.db",
        ),
    )

    entity_df = pd.DataFrame({
        "driver": [1, 2],
        "event_timestamp": [datetime(2026, 1, 16, 12, 0, 0)] * 2,
    })

    # Mock catalog and table operations
    mock_table = MagicMock()
    mock_table.scan.return_value.plan_files.return_value = []
    mock_table.schema.return_value.as_arrow.return_value = MagicMock()

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
            mock_con = MagicMock()
            mock_duckdb.return_value = mock_con

            retrieval_job = IcebergOfflineStore.get_historical_features(
                config=config,
                feature_views=[feature_view],
                feature_refs=["test_fv:feature1"],
                entity_df=entity_df,
                registry=MagicMock(),
                project="test_project",
            )

            # Check that the query contains TTL filtering
            query = retrieval_job.query

            # Should contain the TTL interval filter
            assert "INTERVAL" in query
            assert "86400" in query or "86400.0" in query  # 24 hours * 3600 seconds

            # Should have the correct inequality direction
            # feature_timestamp >= entity_timestamp - INTERVAL 'ttl' SECOND
            assert ">=" in query
            assert "event_timestamp - INTERVAL" in query


def test_created_timestamp_used_in_pull_latest():
    """Test that created_timestamp is used as tiebreaker in pull_latest_from_table_or_query."""
    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import IcebergSource
    from feast.repo_config import RepoConfig
    from unittest.mock import MagicMock, patch

    source = IcebergSource(
        name="test_source",
        table_identifier="test.features",
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )

    config = RepoConfig(
        project="test_project",
        registry="registry.db",
        provider="local",
        offline_store=IcebergOfflineStoreConfig(
            catalog_type="sql",
            uri="sqlite:///test.db",
        ),
    )

    # Mock catalog and table operations
    mock_table = MagicMock()
    mock_table.scan.return_value.plan_files.return_value = []
    mock_table.schema.return_value.as_arrow.return_value = MagicMock()

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
            mock_con = MagicMock()
            mock_duckdb.return_value = mock_con

            retrieval_job = IcebergOfflineStore.pull_latest_from_table_or_query(
                config=config,
                data_source=source,
                join_key_columns=["driver_id"],
                feature_name_columns=["feature1"],
                timestamp_field="event_timestamp",
                created_timestamp_column="created_timestamp",
                start_date=None,
                end_date=None,
            )

            # Check that the query includes created_timestamp in ORDER BY
            query = retrieval_job.query

            # Should order by both event_timestamp and created_timestamp
            assert "ORDER BY event_timestamp DESC, created_timestamp DESC" in query or \
                   "ORDER BY event_timestamp DESC,created_timestamp DESC" in query


def test_ttl_filter_not_added_when_ttl_is_none():
    """Test that TTL filter is not added when FeatureView has no TTL."""
    from feast.feature_view import FeatureView
    from feast.field import Field
    from feast.types import Int32
    from feast.entity import Entity
    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import IcebergSource
    from feast.repo_config import RepoConfig
    from unittest.mock import MagicMock, patch

    # Create entity
    driver_entity = Entity(name="driver", join_keys=["driver_id"])

    source = IcebergSource(
        name="test_source",
        table_identifier="test.features",
        timestamp_field="event_timestamp",
    )

    # Feature view WITHOUT TTL
    feature_view = FeatureView(
        name="test_fv",
        entities=[driver_entity],
        schema=[Field(name="feature1", dtype=Int32)],
        source=source,
        ttl=None,  # No TTL
    )

    config = RepoConfig(
        project="test_project",
        registry="registry.db",
        provider="local",
        offline_store=IcebergOfflineStoreConfig(
            catalog_type="sql",
            uri="sqlite:///test.db",
        ),
    )

    entity_df = pd.DataFrame({
        "driver": [1, 2],
        "event_timestamp": [datetime(2026, 1, 16, 12, 0, 0)] * 2,
    })

    mock_table = MagicMock()
    mock_table.scan.return_value.plan_files.return_value = []
    mock_table.schema.return_value.as_arrow.return_value = MagicMock()

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
            mock_con = MagicMock()
            mock_duckdb.return_value = mock_con

            retrieval_job = IcebergOfflineStore.get_historical_features(
                config=config,
                feature_views=[feature_view],
                feature_refs=["test_fv:feature1"],
                entity_df=entity_df,
                registry=MagicMock(),
                project="test_project",
            )

            query = retrieval_job.query

            # Should NOT contain TTL filtering when ttl is None
            # The query should only have the basic ASOF join condition
            assert "ASOF LEFT JOIN" in query
            # TTL-specific interval should not be present
            lines_with_interval = [line for line in query.split('\n') if 'INTERVAL' in line and 'event_timestamp - INTERVAL' in line]
            assert len(lines_with_interval) == 0, "TTL filter should not be present when ttl is None"
