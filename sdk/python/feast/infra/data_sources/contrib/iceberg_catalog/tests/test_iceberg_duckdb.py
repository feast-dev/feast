"""Unit tests for DuckDB offline store reading from IcebergSource."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.infra.offline_stores.duckdb import (
    _read_iceberg_catalog_source,
)


class TestReadIcebergCatalogSource:
    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog"
        ".iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_pyiceberg_scan(self, mock_get_client):
        """Verify data is loaded via PyIceberg scan -> Arrow -> ibis."""
        arrow_tbl = pa.table({"id": [1, 2], "val": [10.0, 20.0]})

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value.to_arrow.return_value = arrow_tbl

        mock_client = MagicMock()
        mock_client.load_table.return_value = mock_iceberg_table
        mock_get_client.return_value = mock_client

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="my_table",
            credential_vending=False,
        )
        result = _read_iceberg_catalog_source(source, repo_path=".")
        result_df = result.execute()
        assert len(result_df) == 2
        assert list(result_df["val"]) == [10.0, 20.0]
        mock_client.load_table.assert_called_once_with("ns.my_table")

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog"
        ".iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_catalog_load_failure_propagates(self, mock_get_client):
        """Verify errors propagate instead of being silently swallowed."""
        mock_client = MagicMock()
        mock_client.load_table.side_effect = Exception("table not found")
        mock_get_client.return_value = mock_client

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        with pytest.raises(Exception, match="table not found"):
            _read_iceberg_catalog_source(source, repo_path=".")

    @patch("pyiceberg.catalog.load_catalog")
    def test_rest_catalog_path(self, mock_load_catalog):
        """Verify REST catalog type uses load_catalog with correct config."""
        arrow_tbl = pa.table({"id": [1], "val": [42.0]})

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value.to_arrow.return_value = arrow_tbl

        mock_catalog = MagicMock()
        mock_catalog.load_table.return_value = mock_iceberg_table
        mock_load_catalog.return_value = mock_catalog

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
            catalog_type="rest",
            credential_vending=False,
        )
        result = _read_iceberg_catalog_source(source, repo_path=".")
        result_df = result.execute()
        assert len(result_df) == 1
        assert result_df["val"].iloc[0] == 42.0
        mock_load_catalog.assert_called_once()
        mock_catalog.load_table.assert_called_once_with("ns.t")
