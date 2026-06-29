"""Unit tests for Spark offline store reading from IcebergSource."""

import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)

pyspark = pytest.importorskip("pyspark")

from feast.infra.offline_stores.contrib.spark_offline_store.spark import (  # noqa: E402
    _pull_from_iceberg_source,
    _register_iceberg_source_as_temp_view,
    _resolve_uc_storage_location,
)


@pytest.fixture(scope="module")
def spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("feast_iceberg_test")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class TestRegisterIcebergSourceAsTempView:
    def test_local_file_mode(self, spark_session):
        """file:// endpoint resolves to repo_path/data/{table}."""
        with tempfile.TemporaryDirectory() as repo_path:
            data_dir = os.path.join(repo_path, "data", "my_table")
            os.makedirs(data_dir)
            df = pd.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(data_dir, "data.parquet"),
            )

            source = IcebergSource(
                endpoint="file://local",
                warehouse="w",
                namespace="ns",
                table="my_table",
            )
            config = MagicMock()
            config.repo_path = repo_path

            _register_iceberg_source_as_temp_view(spark_session, source, config)

            result = spark_session.sql("SELECT * FROM iceberg_tmp_my_table")
            assert result.count() == 3

    @patch(
        "feast.infra.data_sources.contrib.iceberg_catalog"
        ".iceberg_source.IcebergSource.get_catalog_client"
    )
    def test_catalog_resolution(self, mock_get_client, spark_session):
        """Non-file endpoint resolves location via catalog client."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame({"id": [1], "val": [42.0]})
            pq.write_table(
                pa.Table.from_pandas(df),
                os.path.join(tmpdir, "data.parquet"),
            )

            mock_client = MagicMock()
            mock_client.load_table.return_value = MagicMock(location=f"file://{tmpdir}")
            mock_get_client.return_value = mock_client

            source = IcebergSource(
                endpoint="http://host:8080/iceberg",
                warehouse="w",
                namespace="ns",
                table="catalog_tbl",
            )
            config = MagicMock()
            config.repo_path = "."

            _register_iceberg_source_as_temp_view(spark_session, source, config)

            result = spark_session.sql("SELECT * FROM iceberg_tmp_catalog_tbl")
            assert result.count() == 1
            assert result.collect()[0]["val"] == 42.0


class TestResolveUcStorageLocation:
    @patch("requests.get")
    def test_successful_resolution(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"storage_location": "file:///data/table1"}
        mock_get.return_value = mock_resp

        source = IcebergSource(
            endpoint="http://localhost:8080",
            warehouse="unity",
            namespace="default",
            table="table1",
        )

        with patch(
            "feast.infra.offline_stores.contrib.spark_offline_store.spark.requests",
            wraps=None,
        ):
            result = _resolve_uc_storage_location(source)

        assert result == "file:///data/table1"

    @patch("requests.get")
    def test_failed_resolution(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        source = IcebergSource(
            endpoint="http://localhost:8080",
            warehouse="unity",
            namespace="default",
            table="missing_table",
        )

        with patch(
            "feast.infra.offline_stores.contrib.spark_offline_store.spark.requests",
            wraps=None,
        ):
            result = _resolve_uc_storage_location(source)

        assert result is None


class TestPullFromIcebergSource:
    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark"
        "._register_iceberg_source_as_temp_view"
    )
    def test_pull_with_time_filter(self, mock_register, spark_session):
        """Verify _pull_from_iceberg_source creates proper time-filtered query."""
        test_df = spark_session.createDataFrame(
            [
                (1, 0.9, datetime(2023, 1, 1, tzinfo=timezone.utc)),
                (2, 0.8, datetime(2023, 6, 15, tzinfo=timezone.utc)),
                (3, 0.7, datetime(2024, 1, 1, tzinfo=timezone.utc)),
            ],
            ["driver_id", "conv_rate", "event_timestamp"],
        )
        test_df.createOrReplaceTempView("iceberg_tmp_driver_stats")

        source = IcebergSource(
            endpoint="http://host:8080/iceberg",
            warehouse="w",
            namespace="ns",
            table="driver_stats",
        )
        config = MagicMock()

        job = _pull_from_iceberg_source(
            spark_session=spark_session,
            data_source=source,
            join_key_columns=["driver_id"],
            feature_name_columns=["conv_rate"],
            timestamp_field="event_timestamp",
            created_timestamp_column=None,
            start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2023, 12, 31, tzinfo=timezone.utc),
            config=config,
        )

        result_df = job.to_df()
        assert len(result_df) == 2
        assert set(result_df["driver_id"].tolist()) == {1, 2}
