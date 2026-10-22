import json

import pytest

from feast.table_format import (
    DeltaFormat,
    HudiFormat,
    IcebergFormat,
    LanceFormat,
    TableFormatType,
    create_table_format,
    table_format_from_dict,
    table_format_from_json,
    table_format_from_proto,
)


class TestTableFormat:
    """Test core TableFormat classes and functionality."""

    def test_iceberg_table_format_creation(self):
        """Test IcebergFormat creation and properties."""
        iceberg_format = IcebergFormat(
            catalog="my_catalog",
            namespace="my_namespace",
            properties={"catalog.uri": "s3://bucket/warehouse", "format-version": "2"},
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
            checkpoint_location="s3://bucket/checkpoints",
            properties={"delta.autoOptimize.optimizeWrite": "true"},
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
            properties={
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
            properties={"key1": "value1", "key2": "value2"},
        )

        iceberg_dict = iceberg_format.to_dict()
        iceberg_restored = IcebergFormat.from_dict(iceberg_dict)

        assert iceberg_restored.format_type == iceberg_format.format_type
        assert iceberg_restored.catalog == iceberg_format.catalog
        assert iceberg_restored.namespace == iceberg_format.namespace
        assert iceberg_restored.properties == iceberg_format.properties

        # Test Delta
        delta_format = DeltaFormat(
            checkpoint_location="s3://bucket/checkpoints",
            properties={"key": "value"},
        )

        delta_dict = delta_format.to_dict()
        delta_restored = DeltaFormat.from_dict(delta_dict)

        assert delta_restored.format_type == delta_format.format_type
        assert delta_restored.properties == delta_format.properties
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
            "properties": {"key1": "value1", "key2": "value2"},
        }
        iceberg_format = table_format_from_dict(iceberg_dict)
        assert isinstance(iceberg_format, IcebergFormat)
        assert iceberg_format.catalog == "test_catalog"

        # Test Delta
        delta_dict = {
            "format_type": "delta",
            "properties": {"key": "value"},
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
            "properties": {},
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
            "properties": {},
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
        delta_format = DeltaFormat(properties=None)
        assert len(delta_format.properties) == 0

        # Test None values in constructors
        hudi_format = HudiFormat(
            table_type=None,
            record_key=None,
            precombine_field=None,
            properties=None,
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
            properties={"custom.prop": "value"},
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
            properties={"special.key": "value with spaces & symbols!@#$%^&*()"},
        )

        # Serialization roundtrip should preserve special characters
        serialized = iceberg_format.to_dict()
        restored = IcebergFormat.from_dict(serialized)
        assert restored.catalog == "测试目录"
        assert restored.namespace == "тест_ns"
        assert (
            restored.properties["special.key"]
            == "value with spaces & symbols!@#$%^&*()"
        )


class TestLanceFormat:
    """Test LanceFormat, including version/tag pinning."""

    def test_lance_table_format_creation(self):
        """Test LanceFormat creation and properties."""
        lance_format = LanceFormat(
            catalog="my_catalog",
            namespace="my_namespace",
            properties={"storage.block_size": "8192"},
        )

        assert lance_format.format_type == TableFormatType.LANCE
        assert lance_format.catalog == "my_catalog"
        assert lance_format.namespace == "my_namespace"
        assert lance_format.properties["lance.catalog"] == "my_catalog"
        assert lance_format.properties["lance.namespace"] == "my_namespace"
        assert lance_format.properties["storage.block_size"] == "8192"

    def test_lance_table_format_minimal(self):
        """Test LanceFormat with no arguments."""
        lance_format = LanceFormat()

        assert lance_format.format_type == TableFormatType.LANCE
        assert lance_format.catalog is None
        assert lance_format.namespace is None
        assert lance_format.version is None
        assert lance_format.tag is None
        assert lance_format.is_pinned is False
        assert lance_format.properties == {}

    def test_lance_version_pin(self):
        """A version pin is recorded on the field and mirrored into properties."""
        lance_format = LanceFormat(catalog="c", version=3)

        assert lance_format.version == 3
        assert lance_format.tag is None
        assert lance_format.is_pinned is True
        assert lance_format.properties["lance.version"] == "3"

    def test_lance_tag_pin(self):
        """A tag pin is recorded on the field and mirrored into properties."""
        lance_format = LanceFormat(catalog="c", tag="candidate")

        assert lance_format.tag == "candidate"
        assert lance_format.version is None
        assert lance_format.is_pinned is True
        assert lance_format.properties["lance.tag"] == "candidate"

    def test_lance_rejects_version_below_one(self):
        """Lance versions start at 1, and the proto treats 0 as unset."""
        with pytest.raises(ValueError, match="version must be >= 1"):
            LanceFormat(version=0)

        with pytest.raises(ValueError, match="version must be >= 1"):
            LanceFormat(version=-1)

    def test_lance_rejects_version_and_tag_together(self):
        """A tag already resolves to a version, so both together is ambiguous."""
        with pytest.raises(ValueError, match="either version or tag"):
            LanceFormat(version=1, tag="candidate")

    def test_lance_dict_round_trip(self):
        """to_dict/from_dict preserves addressing and pin."""
        original = LanceFormat(catalog="c", namespace="n", version=7)
        restored = table_format_from_dict(original.to_dict())

        assert isinstance(restored, LanceFormat)
        assert restored.catalog == "c"
        assert restored.namespace == "n"
        assert restored.version == 7
        assert restored.tag is None

    def test_lance_json_round_trip(self):
        """table_format_from_json preserves a tag pin."""
        original = LanceFormat(catalog="c", tag="candidate")
        restored = table_format_from_json(json.dumps(original.to_dict()))

        assert isinstance(restored, LanceFormat)
        assert restored.tag == "candidate"
        assert restored.version is None

    def test_lance_proto_round_trip(self):
        """to_proto/from_proto selects the lance_format oneof and keeps the pin."""
        original = LanceFormat(catalog="c", namespace="n", version=3)
        proto = original.to_proto()

        assert proto.WhichOneof("format") == "lance_format"

        restored = table_format_from_proto(proto)
        assert isinstance(restored, LanceFormat)
        assert restored.catalog == "c"
        assert restored.namespace == "n"
        assert restored.version == 3
        assert restored.tag is None

    def test_lance_proto_round_trip_unpinned(self):
        """An unpinned format must not come back pinned to version 0."""
        restored = table_format_from_proto(LanceFormat(catalog="c").to_proto())

        assert isinstance(restored, LanceFormat)
        assert restored.version is None
        assert restored.tag is None
        assert restored.is_pinned is False

    def test_lance_factory_function(self):
        """create_table_format dispatches to LanceFormat."""
        lance_format = create_table_format(
            TableFormatType.LANCE, catalog="c", version=2
        )

        assert isinstance(lance_format, LanceFormat)
        assert lance_format.version == 2
