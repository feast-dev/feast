"""Unit tests for IcebergSource offline_write_batch support.

Tests that SparkOfflineStore and DuckDB _write_data_source correctly
route IcebergSource writes through PyIceberg's catalog-aware append/overwrite
API instead of raw file writes.
"""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)


@pytest.fixture
def sample_arrow_table():
    """A minimal Arrow table representing feature data."""
    return pa.table(
        {
            "entity_id": pa.array([1, 2, 3]),
            "feature_value": pa.array([10.0, 20.0, 30.0]),
            "event_timestamp": pa.array(
                ["2026-01-01T00:00:00", "2026-01-02T00:00:00", "2026-01-03T00:00:00"]
            ),
        }
    )


@pytest.fixture
def iceberg_source():
    """An IcebergSource with REST catalog for testing."""
    return IcebergSource(
        endpoint="http://localhost:8181",
        warehouse="test_warehouse",
        namespace="test_ns",
        table="test_features",
        catalog_type="rest",
        catalog_name="feast_test",
        timestamp_field="event_timestamp",
    )


@pytest.fixture
def iceberg_source_hive():
    """An IcebergSource with Hive catalog for testing."""
    return IcebergSource(
        catalog_type="hive",
        warehouse="hive_warehouse",
        namespace="hive_ns",
        table="hive_features",
        catalog_name="hive_catalog",
        catalog_properties={"uri": "thrift://metastore:9083"},
        timestamp_field="event_timestamp",
    )


class TestSparkOfflineStoreIcebergWrite:
    """Tests for _iceberg_offline_write_batch in the Spark offline store."""

    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark"
        "._load_pyiceberg_table"
    )
    def test_append_calls_pyiceberg(
        self, mock_load_table, iceberg_source, sample_arrow_table
    ):
        """Verify that IcebergSource write calls PyIceberg append()."""
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _iceberg_offline_write_batch,
        )

        mock_iceberg_table = MagicMock()
        mock_load_table.return_value = mock_iceberg_table

        feature_view = MagicMock()
        feature_view.batch_source = iceberg_source

        config = MagicMock()

        _iceberg_offline_write_batch(config, feature_view, sample_arrow_table)

        mock_load_table.assert_called_once_with(iceberg_source)
        mock_iceberg_table.append.assert_called_once_with(sample_arrow_table)

    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark"
        "._load_pyiceberg_table"
    )
    def test_passes_correct_data_source(
        self, mock_load_table, iceberg_source_hive, sample_arrow_table
    ):
        """Verify Hive catalog IcebergSource is passed to _load_pyiceberg_table."""
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _iceberg_offline_write_batch,
        )

        mock_iceberg_table = MagicMock()
        mock_load_table.return_value = mock_iceberg_table

        feature_view = MagicMock()
        feature_view.batch_source = iceberg_source_hive

        config = MagicMock()

        _iceberg_offline_write_batch(config, feature_view, sample_arrow_table)

        mock_load_table.assert_called_once_with(iceberg_source_hive)
        mock_iceberg_table.append.assert_called_once()

    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark"
        "._load_pyiceberg_table"
    )
    def test_append_error_propagates(
        self, mock_load_table, iceberg_source, sample_arrow_table
    ):
        """Verify that PyIceberg errors during append propagate."""
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            _iceberg_offline_write_batch,
        )

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.append.side_effect = RuntimeError("Catalog commit failed")
        mock_load_table.return_value = mock_iceberg_table

        feature_view = MagicMock()
        feature_view.batch_source = iceberg_source

        config = MagicMock()

        with pytest.raises(RuntimeError, match="Catalog commit failed"):
            _iceberg_offline_write_batch(config, feature_view, sample_arrow_table)

    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark"
        "._load_pyiceberg_table"
    )
    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark"
        ".get_spark_session_or_start_new_with_repoconfig"
    )
    def test_offline_write_batch_dispatches_iceberg(
        self,
        mock_get_spark,
        mock_load_table,
        iceberg_source,
        sample_arrow_table,
    ):
        """End-to-end: SparkOfflineStore.offline_write_batch dispatches to
        _iceberg_offline_write_batch when batch_source is IcebergSource."""
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            SparkOfflineStore,
            SparkOfflineStoreConfig,
        )

        mock_iceberg_table = MagicMock()
        mock_load_table.return_value = mock_iceberg_table

        feature_view = MagicMock()
        feature_view.batch_source = iceberg_source

        config = MagicMock()
        config.offline_store = SparkOfflineStoreConfig()

        SparkOfflineStore.offline_write_batch(
            config, feature_view, sample_arrow_table, progress=None
        )

        mock_load_table.assert_called_once_with(iceberg_source)
        mock_iceberg_table.append.assert_called_once_with(sample_arrow_table)
        mock_get_spark.assert_not_called()

    def test_offline_write_batch_rejects_non_spark_non_iceberg(self):
        """SparkOfflineStore.offline_write_batch raises for unknown source types."""
        from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
            SparkOfflineStore,
            SparkOfflineStoreConfig,
        )

        feature_view = MagicMock()
        feature_view.batch_source = MagicMock()
        feature_view.batch_source.__class__ = type("UnknownSource", (), {})

        config = MagicMock()
        config.offline_store = SparkOfflineStoreConfig()

        with pytest.raises(AssertionError):
            SparkOfflineStore.offline_write_batch(
                config,
                feature_view,
                pa.table({"a": [1]}),
                progress=None,
            )


class TestDuckDBOfflineStoreIcebergWrite:
    """Tests for _write_iceberg_data_source in the DuckDB offline store."""

    @patch("feast.infra.offline_stores.duckdb._load_pyiceberg_table_for_write")
    def test_append_mode(self, mock_load_table, iceberg_source, sample_arrow_table):
        """Verify append mode calls iceberg_table.append()."""
        from feast.infra.offline_stores.duckdb import _write_iceberg_data_source

        mock_iceberg_table = MagicMock()
        mock_load_table.return_value = mock_iceberg_table

        import ibis

        ibis_table = ibis.memtable(sample_arrow_table)

        _write_iceberg_data_source(ibis_table, iceberg_source, mode="append")

        mock_load_table.assert_called_once_with(iceberg_source)
        mock_iceberg_table.append.assert_called_once()
        mock_iceberg_table.overwrite.assert_not_called()

    @patch("feast.infra.offline_stores.duckdb._load_pyiceberg_table_for_write")
    def test_overwrite_mode(self, mock_load_table, iceberg_source, sample_arrow_table):
        """Verify overwrite mode calls iceberg_table.overwrite()."""
        from feast.infra.offline_stores.duckdb import _write_iceberg_data_source

        mock_iceberg_table = MagicMock()
        mock_load_table.return_value = mock_iceberg_table

        import ibis

        ibis_table = ibis.memtable(sample_arrow_table)

        _write_iceberg_data_source(ibis_table, iceberg_source, mode="overwrite")

        mock_load_table.assert_called_once_with(iceberg_source)
        mock_iceberg_table.overwrite.assert_called_once()
        mock_iceberg_table.append.assert_not_called()

    @patch("feast.infra.offline_stores.duckdb._load_pyiceberg_table_for_write")
    def test_write_data_source_dispatches_iceberg(
        self, mock_load_table, iceberg_source, sample_arrow_table
    ):
        """_write_data_source dispatches to _write_iceberg_data_source for IcebergSource."""
        from feast.infra.offline_stores.duckdb import _write_data_source

        mock_iceberg_table = MagicMock()
        mock_load_table.return_value = mock_iceberg_table

        import ibis

        ibis_table = ibis.memtable(sample_arrow_table)

        _write_data_source(ibis_table, iceberg_source, repo_path="/tmp/repo")

        mock_load_table.assert_called_once_with(iceberg_source)
        mock_iceberg_table.append.assert_called_once()

    @patch("feast.infra.offline_stores.duckdb._load_pyiceberg_table_for_write")
    def test_error_propagates(
        self, mock_load_table, iceberg_source, sample_arrow_table
    ):
        """Verify that PyIceberg errors propagate through DuckDB path."""
        from feast.infra.offline_stores.duckdb import _write_iceberg_data_source

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.append.side_effect = RuntimeError("Schema mismatch")
        mock_load_table.return_value = mock_iceberg_table

        import ibis

        ibis_table = ibis.memtable(sample_arrow_table)

        with pytest.raises(RuntimeError, match="Schema mismatch"):
            _write_iceberg_data_source(ibis_table, iceberg_source)
