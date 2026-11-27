"""Unit tests for Spark offline store reading from IcebergSource."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)

pyspark = pytest.importorskip("pyspark")

from feast.infra.offline_stores.contrib.spark_offline_store.spark import (  # noqa: E402
    _pull_from_iceberg_source,
    _register_iceberg_source_as_temp_view,
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
    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store"
        ".spark._load_pyiceberg_table"
    )
    def test_pyiceberg_scan(self, mock_load, spark_session):
        """Verify data is loaded via PyIceberg scan -> Arrow -> Spark DataFrame."""
        arrow_tbl = pa.table({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})

        mock_iceberg_table = MagicMock()
        mock_iceberg_table.scan.return_value.to_arrow.return_value = arrow_tbl
        mock_load.return_value = mock_iceberg_table

        source = IcebergSource(
            endpoint="http://host:8080/iceberg",
            warehouse="w",
            namespace="ns",
            table="my_table",
        )
        config = MagicMock()

        _register_iceberg_source_as_temp_view(spark_session, source, config)

        result = spark_session.sql("SELECT * FROM iceberg_tmp_my_table")
        assert result.count() == 3
        mock_load.assert_called_once_with(source)

    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store"
        ".spark._load_pyiceberg_table"
    )
    def test_catalog_load_failure_propagates(self, mock_load, spark_session):
        """Verify errors propagate instead of being silently swallowed."""
        mock_load.side_effect = Exception("table not found")

        source = IcebergSource(
            endpoint="http://host:8080/iceberg",
            warehouse="w",
            namespace="ns",
            table="bad_table",
        )
        config = MagicMock()

        with pytest.raises(Exception, match="table not found"):
            _register_iceberg_source_as_temp_view(spark_session, source, config)


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
