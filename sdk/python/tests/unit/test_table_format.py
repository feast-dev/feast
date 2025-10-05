import json

import pytest

from feast.table_format import (
    DeltaFormat,
    HudiFormat,
    IcebergFormat,
    TableFormatType,
    create_table_format,
    table_format_from_dict,
    table_format_from_json,
)


class TestTableFormat:
    """Test core TableFormat classes and functionality."""

    def test_iceberg_table_format_creation(self):
        """Test IcebergFormat creation and properties."""
        iceberg_format = IcebergFormat(
            catalog="my_catalog",
            namespace="my_namespace",
            catalog_properties={"catalog.uri": "s3://bucket/warehouse"},
            table_properties={"format-version": "2"},
        )

        assert iceberg_format.format_type == TableFormatType.ICEBERG
        assert iceberg_format.catalog == "my_catalog"
        assert iceberg_format.namespace == "my_namespace"
        assert iceberg_format.get_property("iceberg.catalog") == "my_catalog"
        assert iceberg_format.get_property("iceberg.namespace") == "my_namespace"
        assert iceberg_format.get_property("catalog.uri") == "s3://bucket/warehouse"
        assert iceberg_format.get_property("format-version") == "2"

    def test_iceberg_table_format_minimal(self):
        """Test IcebergFormat with minimal config."""
        iceberg_format = IcebergFormat()

        assert iceberg_format.format_type == TableFormatType.ICEBERG
        assert iceberg_format.catalog is None
        assert iceberg_format.namespace is None
        assert len(iceberg_format.properties) == 0

    def test_delta_table_format_creation(self):
        """Test DeltaFormat creation and properties."""
        delta_format = DeltaFormat(
            table_properties={"delta.autoOptimize.optimizeWrite": "true"},
            checkpoint_location="s3://bucket/checkpoints",
        )

        assert delta_format.format_type == TableFormatType.DELTA
        assert delta_format.checkpoint_location == "s3://bucket/checkpoints"
        assert (
            delta_format.get_property("delta.checkpointLocation")
            == "s3://bucket/checkpoints"
        )
        assert delta_format.get_property("delta.autoOptimize.optimizeWrite") == "true"

    def test_hudi_table_format_creation(self):
        """Test HudiFormat creation and properties."""
        hudi_format = HudiFormat(
            table_type="COPY_ON_WRITE",
            record_key="id",
            precombine_field="timestamp",
            table_properties={
                "hoodie.compaction.strategy": "org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy"
            },
        )

        assert hudi_format.format_type == TableFormatType.HUDI
        assert hudi_format.table_type == "COPY_ON_WRITE"
        assert hudi_format.record_key == "id"
        assert hudi_format.precombine_field == "timestamp"
        assert (
            hudi_format.get_property("hoodie.datasource.write.table.type")
            == "COPY_ON_WRITE"
        )
        assert (
            hudi_format.get_property("hoodie.datasource.write.recordkey.field") == "id"
        )
        assert (
            hudi_format.get_property("hoodie.datasource.write.precombine.field")
            == "timestamp"
        )

    def test_table_format_property_methods(self):
        """Test property getter/setter methods."""
        iceberg_format = IcebergFormat()

        # Test setting and getting properties
        iceberg_format.set_property("snapshot-id", "123456789")
        assert iceberg_format.get_property("snapshot-id") == "123456789"

        # Test default value
        assert iceberg_format.get_property("non-existent-key", "default") == "default"
        assert iceberg_format.get_property("non-existent-key") is None

    def test_table_format_serialization(self):
        """Test table format serialization to/from dict."""
        # Test Iceberg
        iceberg_format = IcebergFormat(
            catalog="test_catalog",
            namespace="test_namespace",
            catalog_properties={"key1": "value1"},
            table_properties={"key2": "value2"},
        )

        iceberg_dict = iceberg_format.to_dict()
        iceberg_restored = IcebergFormat.from_dict(iceberg_dict)

        assert iceberg_restored.format_type == iceberg_format.format_type
        assert iceberg_restored.catalog == iceberg_format.catalog
        assert iceberg_restored.namespace == iceberg_format.namespace
        assert iceberg_restored.catalog_properties == iceberg_format.catalog_properties
        assert iceberg_restored.table_properties == iceberg_format.table_properties

        # Test Delta
        delta_format = DeltaFormat(
            table_properties={"key": "value"},
            checkpoint_location="s3://bucket/checkpoints",
        )

        delta_dict = delta_format.to_dict()
        delta_restored = DeltaFormat.from_dict(delta_dict)

        assert delta_restored.format_type == delta_format.format_type
        assert delta_restored.table_properties == delta_format.table_properties
        assert delta_restored.checkpoint_location == delta_format.checkpoint_location

        # Test Hudi
        hudi_format = HudiFormat(
            table_type="MERGE_ON_READ",
            record_key="uuid",
            precombine_field="ts",
        )

        hudi_dict = hudi_format.to_dict()
        hudi_restored = HudiFormat.from_dict(hudi_dict)

        assert hudi_restored.format_type == hudi_format.format_type
        assert hudi_restored.table_type == hudi_format.table_type
        assert hudi_restored.record_key == hudi_format.record_key
        assert hudi_restored.precombine_field == hudi_format.precombine_field

    def test_factory_function(self):
        """Test create_table_format factory function."""
        # Test Iceberg
        iceberg_format = create_table_format(
            TableFormatType.ICEBERG,
            catalog="test_catalog",
            namespace="test_ns",
        )
        assert isinstance(iceberg_format, IcebergFormat)
        assert iceberg_format.catalog == "test_catalog"
        assert iceberg_format.namespace == "test_ns"

        # Test Delta
        delta_format = create_table_format(
            TableFormatType.DELTA,
            checkpoint_location="s3://test",
        )
        assert isinstance(delta_format, DeltaFormat)
        assert delta_format.checkpoint_location == "s3://test"

        # Test Hudi
        hudi_format = create_table_format(
            TableFormatType.HUDI,
            table_type="COPY_ON_WRITE",
        )
        assert isinstance(hudi_format, HudiFormat)
        assert hudi_format.table_type == "COPY_ON_WRITE"

        # Test invalid format type
        with pytest.raises(ValueError, match="Unknown table format type"):
            create_table_format("invalid_format")

    def test_table_format_from_dict(self):
        """Test table_format_from_dict function."""
        # Test Iceberg
        iceberg_dict = {
            "format_type": "iceberg",
            "catalog": "test_catalog",
            "namespace": "test_namespace",
            "catalog_properties": {"key1": "value1"},
            "table_properties": {"key2": "value2"},
        }
        iceberg_format = table_format_from_dict(iceberg_dict)
        assert isinstance(iceberg_format, IcebergFormat)
        assert iceberg_format.catalog == "test_catalog"

        # Test Delta
        delta_dict = {
            "format_type": "delta",
            "table_properties": {"key": "value"},
            "checkpoint_location": "s3://bucket/checkpoints",
        }
        delta_format = table_format_from_dict(delta_dict)
        assert isinstance(delta_format, DeltaFormat)
        assert delta_format.checkpoint_location == "s3://bucket/checkpoints"

        # Test Hudi
        hudi_dict = {
            "format_type": "hudi",
            "table_type": "MERGE_ON_READ",
            "record_key": "id",
            "precombine_field": "ts",
            "table_properties": {},
        }
        hudi_format = table_format_from_dict(hudi_dict)
        assert isinstance(hudi_format, HudiFormat)
        assert hudi_format.table_type == "MERGE_ON_READ"

        # Test invalid format type
        with pytest.raises(ValueError, match="Unknown table format type"):
            table_format_from_dict({"format_type": "invalid"})

    def test_table_format_from_json(self):
        """Test table_format_from_json function."""
        iceberg_dict = {
            "format_type": "iceberg",
            "catalog": "test_catalog",
            "namespace": "test_namespace",
            "catalog_properties": {},
            "table_properties": {},
        }
        json_str = json.dumps(iceberg_dict)
        iceberg_format = table_format_from_json(json_str)

        assert isinstance(iceberg_format, IcebergFormat)
        assert iceberg_format.catalog == "test_catalog"
        assert iceberg_format.namespace == "test_namespace"

    def test_table_format_error_handling(self):
        """Test error handling in table format operations."""

        # Test invalid format type - create mock enum value
        class MockFormat:
            value = "invalid_format"

        with pytest.raises(ValueError, match="Unknown table format type"):
            create_table_format(MockFormat())

        # Test invalid format type in from_dict
        with pytest.raises(ValueError, match="Unknown table format type"):
            table_format_from_dict({"format_type": "invalid"})

        # Test missing format_type
        with pytest.raises(KeyError):
            table_format_from_dict({})

        # Test invalid JSON
        with pytest.raises(json.JSONDecodeError):
            table_format_from_json("invalid json")

    def test_table_format_property_edge_cases(self):
        """Test edge cases for table format properties."""
        iceberg_format = IcebergFormat()

        # Test property overwriting
        iceberg_format.set_property("snapshot-id", "123")
        assert iceberg_format.get_property("snapshot-id") == "123"
        iceberg_format.set_property("snapshot-id", "456")
        assert iceberg_format.get_property("snapshot-id") == "456"

        # Test empty properties
        delta_format = DeltaFormat(table_properties=None)
        assert len(delta_format.table_properties) == 0

        # Test None values in constructors
        hudi_format = HudiFormat(
            table_type=None,
            record_key=None,
            precombine_field=None,
            table_properties=None,
        )
        assert hudi_format.table_type is None
        assert hudi_format.record_key is None
        assert hudi_format.precombine_field is None

    def test_hudi_format_comprehensive(self):
        """Test comprehensive Hudi format functionality."""
        # Test with all properties
        hudi_format = HudiFormat(
            table_type="COPY_ON_WRITE",
            record_key="id,uuid",
            precombine_field="ts",
            table_properties={"custom.prop": "value"},
        )

        assert (
            hudi_format.get_property("hoodie.datasource.write.table.type")
            == "COPY_ON_WRITE"
        )
        assert (
            hudi_format.get_property("hoodie.datasource.write.recordkey.field")
            == "id,uuid"
        )
        assert (
            hudi_format.get_property("hoodie.datasource.write.precombine.field") == "ts"
        )
        assert hudi_format.get_property("custom.prop") == "value"

        # Test serialization roundtrip with complex data
        serialized = hudi_format.to_dict()
        restored = HudiFormat.from_dict(serialized)
        assert restored.table_type == hudi_format.table_type
        assert restored.record_key == hudi_format.record_key
        assert restored.precombine_field == hudi_format.precombine_field

    def test_table_format_with_special_characters(self):
        """Test table formats with special characters and edge values."""
        # Test with unicode and special characters
        iceberg_format = IcebergFormat(
            catalog="测试目录",  # Chinese
            namespace="тест_ns",  # Cyrillic
            catalog_properties={"special.key": "value with spaces & symbols!@#$%^&*()"},
        )

        # Serialization roundtrip should preserve special characters
        serialized = iceberg_format.to_dict()
        restored = IcebergFormat.from_dict(serialized)
        assert restored.catalog == "测试目录"
        assert restored.namespace == "тест_ns"
        assert (
            restored.catalog_properties["special.key"]
            == "value with spaces & symbols!@#$%^&*()"
        )
