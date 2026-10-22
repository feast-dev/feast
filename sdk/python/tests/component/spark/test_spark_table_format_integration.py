from unittest.mock import MagicMock, patch

import pytest

from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkOptions,
    SparkSource,
)
from feast.table_format import (
    DeltaFormat,
    HudiFormat,
    IcebergFormat,
)


class TestSparkSourceWithTableFormat:
    """Test SparkSource integration with TableFormat."""

    def test_spark_source_with_iceberg_table_format(self):
        """Test SparkSource with IcebergFormat."""
        iceberg_format = IcebergFormat(
            catalog="my_catalog",
            namespace="my_db",
        )
        iceberg_format.set_property("snapshot-id", "123456789")

        spark_source = SparkSource(
            name="iceberg_features",
            path="my_catalog.my_db.my_table",
            table_format=iceberg_format,
        )

        assert spark_source.table_format == iceberg_format
        assert spark_source.table_format.catalog == "my_catalog"
        assert spark_source.table_format.get_property("snapshot-id") == "123456789"
        assert spark_source.path == "my_catalog.my_db.my_table"

    def test_spark_source_with_delta_table_format(self):
        """Test SparkSource with DeltaFormat."""
        delta_format = DeltaFormat()
        delta_format.set_property("versionAsOf", "1")

        spark_source = SparkSource(
            name="delta_features",
            path="s3://bucket/delta-table",
            table_format=delta_format,
        )

        assert spark_source.table_format == delta_format
        assert spark_source.table_format.get_property("versionAsOf") == "1"
        assert spark_source.path == "s3://bucket/delta-table"

    def test_spark_source_with_hudi_table_format(self):
        """Test SparkSource with HudiFormat."""
        hudi_format = HudiFormat(
            table_type="COPY_ON_WRITE",
            record_key="id",
        )
        hudi_format.set_property("hoodie.datasource.query.type", "snapshot")

        spark_source = SparkSource(
            name="hudi_features",
            path="s3://bucket/hudi-table",
            table_format=hudi_format,
        )

        assert spark_source.table_format == hudi_format
        assert spark_source.table_format.table_type == "COPY_ON_WRITE"
        assert (
            spark_source.table_format.get_property("hoodie.datasource.query.type")
            == "snapshot"
        )

    def test_spark_source_without_table_format(self):
        """Test SparkSource without table format (traditional file reading)."""
        spark_source = SparkSource(
            name="parquet_features",
            path="s3://bucket/data.parquet",
            file_format="parquet",
        )

        assert spark_source.table_format is None
        assert spark_source.file_format == "parquet"
        assert spark_source.path == "s3://bucket/data.parquet"

    def test_spark_source_both_file_and_table_format(self):
        """Test SparkSource with both file_format and table_format."""
        iceberg_format = IcebergFormat()

        spark_source = SparkSource(
            name="mixed_features",
            path="s3://bucket/iceberg-table",
            file_format="parquet",  # Underlying file format
            table_format=iceberg_format,  # Table metadata format
        )

        assert spark_source.table_format == iceberg_format
        assert spark_source.file_format == "parquet"

    def test_spark_source_validation_with_table_format(self):
        """Test SparkSource validation with table_format."""
        iceberg_format = IcebergFormat()

        # Should work: path with table_format, no file_format
        spark_source = SparkSource(
            name="iceberg_table",
            path="catalog.db.table",
            table_format=iceberg_format,
        )
        assert spark_source.table_format == iceberg_format

        # Should work: path with both table_format and file_format
        spark_source = SparkSource(
            name="iceberg_table_with_file_format",
            path="s3://bucket/data",
            file_format="parquet",
            table_format=iceberg_format,
        )
        assert spark_source.table_format == iceberg_format
        assert spark_source.file_format == "parquet"

    def test_spark_source_validation_without_table_format(self):
        """Test SparkSource validation without table_format."""
        # Should work: path with file_format, no table_format
        spark_source = SparkSource(
            name="parquet_file",
            path="s3://bucket/data.parquet",
            file_format="parquet",
        )
        assert spark_source.file_format == "parquet"
        assert spark_source.table_format is None

        # Should fail: path without file_format or table_format
        with pytest.raises(
            ValueError,
            match="If 'path' is specified without 'table_format', then 'file_format' is required",
        ):
            SparkSource(
                name="invalid_source",
                path="s3://bucket/data",
            )

    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
    )
    def test_load_dataframe_from_path_with_table_format(self, mock_get_spark_session):
        """Test _load_dataframe_from_path with table formats."""
        mock_spark_session = MagicMock()
        mock_get_spark_session.getActiveSession.return_value = mock_spark_session

        mock_reader = MagicMock()
        mock_spark_session.read.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        # Test Iceberg with options
        iceberg_format = IcebergFormat()
        iceberg_format.set_property("snapshot-id", "123456789")
        iceberg_format.set_property("read.split.target-size", "134217728")

        spark_source = SparkSource(
            name="iceberg_test",
            path="catalog.db.table",
            table_format=iceberg_format,
        )

        spark_source._load_dataframe_from_path(mock_spark_session)

        # Verify format was set to iceberg
        mock_spark_session.read.format.assert_called_with("iceberg")

        # Verify options were set
        mock_reader.option.assert_any_call("snapshot-id", "123456789")
        mock_reader.option.assert_any_call("read.split.target-size", "134217728")

        # Verify load was called with the path
        mock_reader.load.assert_called_with("catalog.db.table")

    @patch(
        "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
    )
    def test_load_dataframe_from_path_without_table_format(
        self, mock_get_spark_session
    ):
        """Test _load_dataframe_from_path without table formats."""
        mock_spark_session = MagicMock()
        mock_get_spark_session.getActiveSession.return_value = mock_spark_session

        mock_reader = MagicMock()
        mock_spark_session.read.format.return_value = mock_reader
        mock_reader.load.return_value = MagicMock()

        spark_source = SparkSource(
            name="parquet_test",
            path="s3://bucket/data.parquet",
            file_format="parquet",
        )

        spark_source._load_dataframe_from_path(mock_spark_session)

        # Verify format was set to parquet (file format)
        mock_spark_session.read.format.assert_called_with("parquet")

        # Verify load was called with the path
        mock_reader.load.assert_called_with("s3://bucket/data.parquet")


class TestSparkOptionsWithTableFormat:
    """Test SparkOptions serialization with TableFormat."""

    def test_spark_options_protobuf_serialization_with_table_format(self):
        """Test SparkOptions protobuf serialization/deserialization with table format."""
        iceberg_format = IcebergFormat(
            catalog="test_catalog",
            namespace="test_namespace",
        )
        iceberg_format.set_property("snapshot-id", "123456789")

        spark_options = SparkOptions(
            table=None,
            query=None,
            path="catalog.db.table",
            file_format=None,
            table_format=iceberg_format,
        )

        # Test serialization to proto
        proto = spark_options.to_proto()
        assert proto.path == "catalog.db.table"
        assert proto.file_format == ""  # Should be empty when not provided

        # Verify table_format is serialized as proto TableFormat
        assert proto.HasField("table_format")
        assert proto.table_format.HasField("iceberg_format")
        assert proto.table_format.iceberg_format.catalog == "test_catalog"
        assert proto.table_format.iceberg_format.namespace == "test_namespace"
        assert (
            proto.table_format.iceberg_format.properties["snapshot-id"] == "123456789"
        )

        # Test deserialization from proto
        restored_options = SparkOptions.from_proto(proto)
        assert restored_options.path == "catalog.db.table"
        assert restored_options.file_format == ""
        assert isinstance(restored_options.table_format, IcebergFormat)
        assert restored_options.table_format.catalog == "test_catalog"
        assert restored_options.table_format.namespace == "test_namespace"
        assert restored_options.table_format.get_property("snapshot-id") == "123456789"

    def test_spark_options_protobuf_serialization_without_table_format(self):
        """Test SparkOptions protobuf serialization/deserialization without table format."""
        spark_options = SparkOptions(
            table=None,
            query=None,
            path="s3://bucket/data.parquet",
            file_format="parquet",
            table_format=None,
        )

        # Test serialization to proto
        proto = spark_options.to_proto()
        assert proto.path == "s3://bucket/data.parquet"
        assert proto.file_format == "parquet"
        assert not proto.HasField("table_format")  # Should not have table_format field

        # Test deserialization from proto
        restored_options = SparkOptions.from_proto(proto)
        assert restored_options.path == "s3://bucket/data.parquet"
        assert restored_options.file_format == "parquet"
        assert restored_options.table_format is None

    def test_spark_source_protobuf_roundtrip_with_table_format(self):
        """Test complete SparkSource protobuf roundtrip with table format."""
        delta_format = DeltaFormat()
        delta_format.set_property("versionAsOf", "1")

        original_source = SparkSource(
            name="delta_test",
            path="s3://bucket/delta-table",
            table_format=delta_format,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at",
            description="Test delta source",
        )

        # Serialize to proto
        proto = original_source._to_proto_impl()

        # Deserialize from proto
        restored_source = SparkSource.from_proto(proto)

        assert restored_source.name == original_source.name
        assert restored_source.path == original_source.path
        assert restored_source.timestamp_field == original_source.timestamp_field
        assert (
            restored_source.created_timestamp_column
            == original_source.created_timestamp_column
        )
        assert restored_source.description == original_source.description

        # Verify table_format is properly restored
        assert isinstance(restored_source.table_format, DeltaFormat)
        assert restored_source.table_format.get_property("versionAsOf") == "1"
