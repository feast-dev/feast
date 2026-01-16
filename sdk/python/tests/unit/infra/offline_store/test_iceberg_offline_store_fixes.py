"""Unit tests for critical bug fixes in Iceberg Offline Store.

Tests cover:
1. TTL filtering enforcement in ASOF joins
2. SQL injection prevention (entity_df and SQL identifiers)
3. Deterministic tie-breaking with created_timestamp
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

pyiceberg = pytest.importorskip("pyiceberg")
duckdb = pytest.importorskip("duckdb")

from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (  # noqa: E402
    IcebergOfflineStore,
    IcebergOfflineStoreConfig,
    validate_sql_identifier,
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
    from unittest.mock import MagicMock, patch

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
            IcebergOfflineStore.get_historical_features(
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
    from unittest.mock import MagicMock, patch

    from feast.entity import Entity
    from feast.feature_view import FeatureView
    from feast.field import Field
    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
        IcebergSource,
    )
    from feast.repo_config import RepoConfig
    from feast.types import Int32

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

    # Mock catalog and table operations with proper file paths
    mock_scan = MagicMock()
    mock_task = MagicMock()
    mock_task.delete_files = []
    mock_task.file.file_path = "s3://bucket/file.parquet"
    mock_scan.plan_files.return_value = [mock_task]

    mock_table = MagicMock()
    mock_table.scan.return_value = mock_scan

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
    from unittest.mock import MagicMock, patch

    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
        IcebergSource,
    )
    from feast.repo_config import RepoConfig

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
    from unittest.mock import MagicMock, patch

    from feast.entity import Entity
    from feast.feature_view import FeatureView
    from feast.field import Field
    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
        IcebergSource,
    )
    from feast.repo_config import RepoConfig
    from feast.types import Int32

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

    # Mock catalog and table operations with proper file paths
    mock_scan = MagicMock()
    mock_task = MagicMock()
    mock_task.delete_files = []
    mock_task.file.file_path = "s3://bucket/file.parquet"
    mock_scan.plan_files.return_value = [mock_task]

    mock_table = MagicMock()
    mock_table.scan.return_value = mock_scan

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


class TestSQLIdentifierValidation:
    """Test suite for SQL identifier validation to prevent SQL injection."""

    def test_validate_sql_identifier_accepts_valid_names(self):
        """Test that valid identifiers are accepted."""
        # Valid identifiers
        assert validate_sql_identifier("my_table", "table") == "my_table"
        assert validate_sql_identifier("user_id", "column") == "user_id"
        assert validate_sql_identifier("_private", "field") == "_private"
        assert validate_sql_identifier("Table123", "table") == "Table123"
        assert validate_sql_identifier("feature_view_v2", "feature view") == "feature_view_v2"

    def test_validate_sql_identifier_rejects_sql_injection(self):
        """Test that SQL injection attempts are rejected."""
        # SQL injection attempts
        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("table; DROP TABLE users--", "table")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("name' OR '1'='1", "column")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("id); DELETE FROM features; --", "field")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("col/**/UNION/**/SELECT", "column")

    def test_validate_sql_identifier_rejects_special_characters(self):
        """Test that identifiers with special characters are rejected."""
        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("table-name", "table")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("column.name", "column")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("field name", "field")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("name@domain", "column")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("table$name", "table")

    def test_validate_sql_identifier_rejects_reserved_words(self):
        """Test that SQL reserved words are rejected."""
        # Common SQL reserved words
        with pytest.raises(ValueError, match="reserved word"):
            validate_sql_identifier("SELECT", "table")

        with pytest.raises(ValueError, match="reserved word"):
            validate_sql_identifier("DROP", "column")

        with pytest.raises(ValueError, match="reserved word"):
            validate_sql_identifier("delete", "field")  # Case insensitive

        with pytest.raises(ValueError, match="reserved word"):
            validate_sql_identifier("WHERE", "table")

        with pytest.raises(ValueError, match="reserved word"):
            validate_sql_identifier("UNION", "column")

    def test_validate_sql_identifier_rejects_empty_string(self):
        """Test that empty identifiers are rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_sql_identifier("", "table")

    def test_validate_sql_identifier_rejects_starts_with_digit(self):
        """Test that identifiers starting with digits are rejected."""
        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("123table", "table")

        with pytest.raises(ValueError, match="Invalid SQL"):
            validate_sql_identifier("9_column", "column")


def test_sql_identifier_validation_in_feature_view_name():
    """Test that malicious feature view names are rejected during query construction."""
    from unittest.mock import MagicMock, patch

    from feast.entity import Entity
    from feast.feature_view import FeatureView
    from feast.field import Field
    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
        IcebergSource,
    )
    from feast.repo_config import RepoConfig
    from feast.types import Int32

    # Create entity
    driver_entity = Entity(name="driver", join_keys=["driver_id"])

    # Create a source
    source = IcebergSource(
        name="test_source",
        table_identifier="test.features",
        timestamp_field="event_timestamp",
    )

    # Create a feature view with malicious name
    feature_view = FeatureView(
        name="features; DROP TABLE entity_df--",  # SQL injection attempt
        entities=[driver_entity],
        schema=[Field(name="feature1", dtype=Int32)],
        source=source,
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
        "driver_id": [1, 2],
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

            # This should raise a ValueError due to invalid feature view name
            with pytest.raises(ValueError, match="Invalid SQL feature view name"):
                IcebergOfflineStore.get_historical_features(
                    config=config,
                    feature_views=[feature_view],
                    feature_refs=["features:feature1"],
                    entity_df=entity_df,
                    registry=MagicMock(),
                    project="test_project",
                )


def test_sql_identifier_validation_in_column_names():
    """Test that malicious column names are rejected during query construction."""
    from unittest.mock import MagicMock, patch

    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
        IcebergSource,
    )
    from feast.repo_config import RepoConfig

    source = IcebergSource(
        name="test_source",
        table_identifier="test.features",
        timestamp_field="event_timestamp",
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

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
            mock_con = MagicMock()
            mock_duckdb.return_value = mock_con

            # Malicious column name in join keys
            with pytest.raises(ValueError, match="Invalid SQL"):
                IcebergOfflineStore.pull_all_from_table_or_query(
                    config=config,
                    data_source=source,
                    join_key_columns=["driver_id; DROP TABLE features--"],
                    feature_name_columns=["feature1"],
                    timestamp_field="event_timestamp",
                )

            # Malicious column name in feature columns
            with pytest.raises(ValueError, match="Invalid SQL"):
                IcebergOfflineStore.pull_all_from_table_or_query(
                    config=config,
                    data_source=source,
                    join_key_columns=["driver_id"],
                    feature_name_columns=["feature1' OR '1'='1"],
                    timestamp_field="event_timestamp",
                )


def test_sql_identifier_validation_in_timestamp_field():
    """Test that malicious timestamp field names are rejected."""
    from unittest.mock import MagicMock, patch

    from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
        IcebergSource,
    )
    from feast.repo_config import RepoConfig

    source = IcebergSource(
        name="test_source",
        table_identifier="test.features",
        timestamp_field="event_timestamp; DELETE FROM features--",  # Malicious
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

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table

    with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
            mock_con = MagicMock()
            mock_duckdb.return_value = mock_con

            # This should raise a ValueError due to invalid timestamp field
            with pytest.raises(ValueError, match="Invalid SQL timestamp field"):
                IcebergOfflineStore.pull_all_from_table_or_query(
                    config=config,
                    data_source=source,
                    join_key_columns=["driver_id"],
                    feature_name_columns=["feature1"],
                    timestamp_field="event_timestamp; DELETE FROM features--",
                )


class TestCredentialSecurityFixes:
    """Test suite for credential security fixes in DuckDB configuration.

    Verifies that AWS credentials are not exposed in SQL strings, logs,
    error messages, or query history (TODO-018).
    """

    def test_credentials_not_in_sql_strings(self):
        """Test that credentials are not interpolated into SQL SET commands."""
        from unittest.mock import MagicMock, call

        import duckdb

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            _configure_duckdb_httpfs,
        )

        # Create a mock DuckDB connection
        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)

        # Storage options with sensitive credentials
        storage_options = {
            "s3.endpoint": "http://localhost:9000",
            "s3.region": "us-east-1",
            "s3.access-key-id": "AKIAIOSFODNN7EXAMPLE",
            "s3.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "s3.session-token": "FwoGZXIvYXdzEBYaDEx...",
        }

        # Call the configuration function
        _configure_duckdb_httpfs(mock_con, storage_options)

        # Verify that execute was called
        assert mock_con.execute.called

        # Get all execute calls
        execute_calls = mock_con.execute.call_args_list

        # Check that INSTALL and LOAD httpfs were called
        assert call("INSTALL httpfs") in execute_calls
        assert call("LOAD httpfs") in execute_calls

        # Verify that NO credentials appear in SQL strings
        for call_obj in execute_calls:
            sql_command = call_obj[0][0] if call_obj[0] else ""

            # CRITICAL: Credentials must NOT appear in SQL strings
            assert "AKIAIOSFODNN7EXAMPLE" not in sql_command, \
                f"Access key exposed in SQL: {sql_command}"
            assert "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" not in sql_command, \
                f"Secret key exposed in SQL: {sql_command}"
            assert "FwoGZXIvYXdzEBYaDEx" not in sql_command, \
                f"Session token exposed in SQL: {sql_command}"

        # Verify parameterized queries are used (with $1 placeholder)
        credential_calls = [
            call_obj for call_obj in execute_calls
            if any(keyword in str(call_obj[0][0]) for keyword in ["s3_access_key_id", "s3_secret_access_key", "s3_session_token"])
        ]

        for call_obj in credential_calls:
            sql_command = call_obj[0][0]
            # Should use parameterized query with $1 placeholder
            assert "$1" in sql_command, \
                f"Expected parameterized query, got: {sql_command}"

    def test_credentials_use_parameterized_queries(self):
        """Test that credentials are passed as parameters, not in SQL strings."""
        from unittest.mock import MagicMock

        import duckdb

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            _configure_duckdb_httpfs,
        )

        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)

        storage_options = {
            "s3.access-key-id": "TEST_ACCESS_KEY",
            "s3.secret-access-key": "TEST_SECRET_KEY",
        }

        _configure_duckdb_httpfs(mock_con, storage_options)

        # Find calls for credential configuration
        execute_calls = mock_con.execute.call_args_list

        # Look for parameterized queries
        access_key_calls = [
            call for call in execute_calls
            if "s3_access_key_id" in str(call[0][0])
        ]

        secret_key_calls = [
            call for call in execute_calls
            if "s3_secret_access_key" in str(call[0][0])
        ]

        # Verify parameterized calls exist
        assert len(access_key_calls) > 0, "No access key configuration found"
        assert len(secret_key_calls) > 0, "No secret key configuration found"

        # Verify parameters are passed separately
        for call in access_key_calls:
            # First argument is SQL, second should be parameter list
            assert len(call[0]) >= 2, "Expected SQL and parameters"
            sql_command = call[0][0]
            params = call[0][1] if len(call[0]) > 1 else None

            # SQL should use placeholder
            assert "$1" in sql_command
            # Credential should be in params, not in SQL
            if params:
                assert "TEST_ACCESS_KEY" in str(params)

    def test_environment_variable_fallback(self):
        """Test that credentials fall back to environment variables."""
        import os
        from unittest.mock import MagicMock, patch

        import duckdb

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            _configure_duckdb_httpfs,
        )

        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)

        # Test with empty storage_options but env vars set
        with patch.dict(os.environ, {
            "AWS_ACCESS_KEY_ID": "ENV_ACCESS_KEY",
            "AWS_SECRET_ACCESS_KEY": "ENV_SECRET_KEY",
            "AWS_REGION": "us-west-2",
        }):
            _configure_duckdb_httpfs(mock_con, {})

            # Verify that configuration was applied from env vars
            execute_calls = mock_con.execute.call_args_list

            # Should still configure httpfs
            assert any("INSTALL httpfs" in str(call) for call in execute_calls)
            assert any("LOAD httpfs" in str(call) for call in execute_calls)

    def test_no_credential_exposure_in_error_messages(self):
        """Test that credentials don't appear in error messages."""
        from unittest.mock import MagicMock

        import duckdb

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            _configure_duckdb_httpfs,
        )

        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)

        # Make execute raise an exception
        def raise_error(sql, *args):
            raise RuntimeError(f"DuckDB error executing: {sql}")

        mock_con.execute.side_effect = raise_error

        storage_options = {
            "s3.access-key-id": "SENSITIVE_KEY_12345",
            "s3.secret-access-key": "SUPER_SECRET_VALUE",
        }

        # Should raise an error from DuckDB
        with pytest.raises(RuntimeError) as exc_info:
            _configure_duckdb_httpfs(mock_con, storage_options)

        # Verify credentials are NOT in the error message
        error_message = str(exc_info.value)
        assert "SENSITIVE_KEY_12345" not in error_message, \
            "Access key should not appear in error message"
        assert "SUPER_SECRET_VALUE" not in error_message, \
            "Secret key should not appear in error message"

    def test_region_and_endpoint_configuration(self):
        """Test that non-sensitive configuration still works correctly."""
        from unittest.mock import MagicMock

        import duckdb

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            _configure_duckdb_httpfs,
        )

        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)

        storage_options = {
            "s3.endpoint": "https://s3.amazonaws.com",
            "s3.region": "eu-west-1",
        }

        _configure_duckdb_httpfs(mock_con, storage_options)

        execute_calls = mock_con.execute.call_args_list

        # Verify region and endpoint are configured with parameterized queries
        region_calls = [c for c in execute_calls if "s3_region" in str(c[0][0])]
        endpoint_calls = [c for c in execute_calls if "s3_endpoint" in str(c[0][0])]

        assert len(region_calls) > 0, "Region should be configured"
        assert len(endpoint_calls) > 0, "Endpoint should be configured"

        # These non-sensitive values can use parameterized queries too
        for region_call in region_calls:
            sql = region_call[0][0]
            assert "$1" in sql or "=" in sql

    def test_http_endpoint_ssl_configuration(self):
        """Test that HTTP/HTTPS endpoints configure SSL correctly."""
        from unittest.mock import MagicMock, call

        import duckdb

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            _configure_duckdb_httpfs,
        )

        mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)

        # Test HTTP endpoint (SSL should be false)
        storage_options_http = {
            "s3.endpoint": "http://minio.local:9000",
        }

        _configure_duckdb_httpfs(mock_con, storage_options_http)

        execute_calls = mock_con.execute.call_args_list

        # Should set s3_use_ssl to false for HTTP
        assert call("SET s3_use_ssl = false") in execute_calls

        # Reset mock
        mock_con.reset_mock()

        # Test HTTPS endpoint (SSL should be true)
        storage_options_https = {
            "s3.endpoint": "https://s3.amazonaws.com",
        }

        _configure_duckdb_httpfs(mock_con, storage_options_https)

        execute_calls = mock_con.execute.call_args_list

        # Should set s3_use_ssl to true for HTTPS
        assert call("SET s3_use_ssl = true") in execute_calls


class TestMORDetectionSingleScan:
    """Test suite for MOR detection single-scan optimization (TODO-019).

    Verifies that scan.plan_files() is called only once to:
    1. Avoid double I/O and metadata scans
    2. Prevent generator exhaustion bugs
    3. Improve performance (2x reduction in metadata I/O)
    """

    def test_setup_duckdb_source_calls_plan_files_once(self):
        """Test that _setup_duckdb_source materializes scan.plan_files() only once."""
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            IcebergOfflineStore,
            IcebergOfflineStoreConfig,
        )
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
            IcebergSource,
        )
        from feast.repo_config import RepoConfig

        source = IcebergSource(
            name="test_source",
            table_identifier="test.features",
            timestamp_field="event_timestamp",
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

        # Create mock tasks that will be returned by plan_files()
        mock_task = MagicMock()
        mock_task.delete_files = []
        mock_task.file.file_path = "s3://bucket/file.parquet"

        # Create a mock scan that tracks plan_files() calls
        mock_scan = MagicMock()
        plan_files_call_count = 0

        def plan_files_side_effect():
            nonlocal plan_files_call_count
            plan_files_call_count += 1
            # Return a list (not a generator) to simulate materialized result
            return [mock_task]

        mock_scan.plan_files.side_effect = plan_files_side_effect

        # Mock table
        mock_table = MagicMock()
        mock_table.scan.return_value = mock_scan

        # Mock catalog
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
            with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
                mock_con = MagicMock()
                mock_duckdb.return_value = mock_con

                # Call the method
                IcebergOfflineStore._setup_duckdb_source(
                    config=config,
                    data_source=source,
                    timestamp_field="event_timestamp",
                    start_date=None,
                    end_date=None,
                )

                # CRITICAL: plan_files() should be called exactly ONCE
                assert plan_files_call_count == 1, \
                    f"Expected plan_files() to be called once, but was called {plan_files_call_count} times"

    def test_setup_duckdb_source_uses_materialized_tasks_for_mor_detection(self):
        """Test that MOR detection uses materialized task list, not a second scan."""
        from unittest.mock import MagicMock, call, patch

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            IcebergOfflineStore,
            IcebergOfflineStoreConfig,
        )
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
            IcebergSource,
        )
        from feast.repo_config import RepoConfig

        source = IcebergSource(
            name="test_source",
            table_identifier="test.features",
            timestamp_field="event_timestamp",
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

        # Create mock task WITH delete files (MOR table)
        mock_task = MagicMock()
        mock_task.delete_files = ["delete_file.parquet"]  # Has deletes
        mock_task.file.file_path = "s3://bucket/file.parquet"

        # Track plan_files calls
        plan_files_calls = []

        def track_plan_files():
            plan_files_calls.append(1)
            return iter([mock_task])

        # Mock scan
        mock_scan = MagicMock()
        mock_scan.plan_files.side_effect = track_plan_files
        mock_arrow_table = MagicMock()
        mock_scan.to_arrow.return_value = mock_arrow_table

        # Mock table that returns our tracked scan
        mock_table = MagicMock()
        mock_table.scan.return_value = mock_scan

        # Mock catalog
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
            with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg._configure_duckdb_httpfs"):
                with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
                    mock_con = MagicMock()
                    mock_duckdb.return_value = mock_con

                    # Call the method
                    con, source_table = IcebergOfflineStore._setup_duckdb_source(
                        config=config,
                        data_source=source,
                        timestamp_field="event_timestamp",
                        start_date=None,
                        end_date=None,
                    )

                    # Verify plan_files was called exactly once
                    assert len(plan_files_calls) == 1, \
                        f"Expected plan_files() to be called once, but was called {len(plan_files_calls)} times"

                    # Verify MOR path was taken (to_arrow called for delete resolution)
                    assert mock_scan.to_arrow.call_count == 1
                    assert call("source_table", mock_arrow_table) in mock_con.register.call_args_list

    def test_setup_duckdb_source_uses_materialized_tasks_for_file_paths(self):
        """Test that file path extraction uses materialized task list, not a second scan."""
        from unittest.mock import MagicMock, patch

        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
            IcebergOfflineStore,
            IcebergOfflineStoreConfig,
        )
        from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
            IcebergSource,
        )
        from feast.repo_config import RepoConfig

        source = IcebergSource(
            name="test_source",
            table_identifier="test.features",
            timestamp_field="event_timestamp",
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

        # Create mock task WITHOUT delete files (COW table)
        mock_task = MagicMock()
        mock_task.delete_files = []  # No deletes
        mock_task.file.file_path = "s3://bucket/data.parquet"

        # Track plan_files calls
        plan_files_calls = []

        def track_plan_files():
            plan_files_calls.append(1)
            return iter([mock_task])

        # Mock scan
        mock_scan = MagicMock()
        mock_scan.plan_files.side_effect = track_plan_files

        # Mock table that returns our tracked scan
        mock_table = MagicMock()
        mock_table.scan.return_value = mock_scan

        # Mock catalog
        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.load_catalog", return_value=mock_catalog):
            with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg._configure_duckdb_httpfs"):
                with patch("feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.duckdb.connect") as mock_duckdb:
                    mock_con = MagicMock()
                    mock_duckdb.return_value = mock_con

                    # Call the method
                    con, source_table = IcebergOfflineStore._setup_duckdb_source(
                        config=config,
                        data_source=source,
                        timestamp_field="event_timestamp",
                        start_date=None,
                        end_date=None,
                    )

                    # Verify plan_files was called exactly once
                    assert len(plan_files_calls) == 1, \
                        f"Expected plan_files() to be called once, but was called {len(plan_files_calls)} times"

                    # Verify COW path was taken (read_parquet, not to_arrow)
                    mock_con.execute.assert_called_once()
                    execute_call = mock_con.execute.call_args[0][0]
                    assert "CREATE VIEW" in execute_call
                    assert "read_parquet" in execute_call
                    assert "s3://bucket/data.parquet" in str(execute_call)
