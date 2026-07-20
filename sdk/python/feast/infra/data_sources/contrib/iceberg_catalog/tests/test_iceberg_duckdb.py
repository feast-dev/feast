"""Unit tests for DuckDB offline store reading from IcebergSource."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.infra.offline_stores.duckdb import (
    _read_iceberg_catalog_source,
    _read_parquet_location,
)


class TestReadParquetLocation:
    def test_read_single_parquet_file(self):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df = pd.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
            df.to_parquet(f.name, index=False)
            try:
                result = _read_parquet_location(f"file:{f.name}")
                result_df = result.execute()
                assert len(result_df) == 3
                assert "id" in result_df.columns
                assert "value" in result_df.columns
            finally:
                os.unlink(f.name)

    def test_read_parquet_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(tmpdir, "part-00000.parquet"),
            )
            result = _read_parquet_location(f"file:{tmpdir}")
            result_df = result.execute()
            assert len(result_df) == 2

    def test_read_nonexistent_location(self):
        with pytest.raises(FileNotFoundError):
            _read_parquet_location("file:///nonexistent/path/nowhere")


class TestReadIcebergCatalogSource:
    def test_local_file_mode(self):
        """Verify file:// endpoint reads from repo_path/data/{table}."""
        with tempfile.TemporaryDirectory() as repo_path:
            data_dir = os.path.join(repo_path, "data", "my_table")
            os.makedirs(data_dir)
            df = pd.DataFrame({"id": [1, 2], "val": [10.0, 20.0]})
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(data_dir, "data.parquet"),
            )

            source = IcebergSource(
                endpoint="file://local",
                warehouse="w",
                namespace="ns",
                table="my_table",
                credential_vending=False,
            )
            result = _read_iceberg_catalog_source(source, repo_path)
            result_df = result.execute()
            assert len(result_df) == 2
            assert result_df["val"].iloc[0] == 10.0

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_catalog_metadata_resolution(self, mock_get_client):
        """Verify the source resolves metadata via catalog before reading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({"id": [1], "val": [42.0]})
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(tmpdir, "data.parquet"),
            )

            mock_client = MagicMock()
            mock_client.load_table.return_value = MagicMock(location=f"file:{tmpdir}")
            mock_get_client.return_value = mock_client

            source = IcebergSource(
                endpoint="http://host/iceberg",
                warehouse="w",
                namespace="ns",
                table="t",
                credential_vending=False,
            )
            result = _read_iceberg_catalog_source(source, repo_path=".")
            result_df = result.execute()
            assert len(result_df) == 1
            assert result_df["val"].iloc[0] == 42.0

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_catalog_resolution_failure(self, mock_get_client):
        """Verify error when table cannot be loaded from catalog."""
        mock_client = MagicMock()
        mock_client.load_table.return_value = None
        mock_get_client.return_value = mock_client

        source = IcebergSource(
            endpoint="http://host/iceberg",
            warehouse="w",
            namespace="ns",
            table="t",
        )
        with pytest.raises(ValueError, match="Cannot load table"):
            _read_iceberg_catalog_source(source, repo_path=".")
