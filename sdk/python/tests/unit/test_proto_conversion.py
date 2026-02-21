# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Comprehensive tests for the proto conversion system.

These tests verify:
1. Core interfaces and registry functionality
2. Value type conversion for all supported types
3. Object converters (Entity, FeatureView, etc.)
4. Error handling and edge cases
5. Backward compatibility with existing APIs
"""

from datetime import datetime, timezone

import numpy as np
import pytest

from feast.entity import Entity
from feast.proto_conversion import (
    EntityConverter,
    ProtoConversionError,
    UnsupportedTypeError,
    ValidationError,
    get_registry,
    get_value_converter,
    proto_value_to_python,
    python_values_to_proto_values,
    register_all_converters,
    reset_registration,
)
from feast.proto_conversion.converters.value_converter import (
    ListTypeHandler,
    MapTypeHandler,
    ScalarTypeHandler,
    SetTypeHandler,
    ValueTypeConverter,
)
from feast.protos.feast.types.Value_pb2 import Value as ProtoValue
from feast.value_type import ValueType


class TestConverterRegistry:
    """Tests for the ConverterRegistry class."""

    def setup_method(self):
        """Reset the registry before each test."""
        reset_registration()

    def teardown_method(self):
        """Clean up after each test."""
        reset_registration()

    def test_singleton_instance(self):
        """Test that get_registry returns the same instance."""
        registry1 = get_registry()
        registry2 = get_registry()
        assert registry1 is registry2

    def test_reset_instance(self):
        """Test that reset creates a new instance."""
        registry1 = get_registry()
        reset_registration()
        registry2 = get_registry()
        assert registry1 is not registry2

    def test_register_converter(self):
        """Test registering a converter."""
        registry = get_registry()
        converter = EntityConverter()
        registry.register(converter)
        assert registry.has_converter(Entity)

    def test_get_converter(self):
        """Test retrieving a registered converter."""
        registry = get_registry()
        converter = EntityConverter()
        registry.register(converter)
        retrieved = registry.get_converter(Entity)
        assert isinstance(retrieved, EntityConverter)

    def test_converter_not_found_error(self):
        """Test that ConverterNotFoundError is raised for unknown types."""
        from feast.proto_conversion import ConverterNotFoundError

        registry = get_registry()
        with pytest.raises(ConverterNotFoundError):
            registry.get_converter(str)


class TestValueTypeConverter:
    """Tests for value type conversion."""

    def setup_method(self):
        """Get a fresh converter instance."""
        self.converter = ValueTypeConverter()

    def test_scalar_int_conversion(self):
        """Test integer scalar conversion."""
        values = [1, 2, 3, None, 5]
        protos = self.converter.to_proto_values(values, ValueType.INT64)
        assert len(protos) == 5
        assert protos[0].int64_val == 1
        assert protos[1].int64_val == 2
        assert protos[3].WhichOneof("val") is None  # None value

    def test_scalar_float_conversion(self):
        """Test float scalar conversion."""
        values = [1.5, 2.5, 3.5]
        protos = self.converter.to_proto_values(values, ValueType.DOUBLE)
        assert len(protos) == 3
        assert protos[0].double_val == 1.5

    def test_scalar_string_conversion(self):
        """Test string scalar conversion."""
        values = ["hello", "world", None]
        protos = self.converter.to_proto_values(values, ValueType.STRING)
        assert len(protos) == 3
        assert protos[0].string_val == "hello"
        assert protos[2].WhichOneof("val") is None

    def test_scalar_bool_conversion(self):
        """Test boolean scalar conversion."""
        values = [True, False, True]
        protos = self.converter.to_proto_values(values, ValueType.BOOL)
        assert len(protos) == 3
        assert protos[0].bool_val is True
        assert protos[1].bool_val is False

    def test_scalar_bytes_conversion(self):
        """Test bytes scalar conversion."""
        values = [b"hello", b"world"]
        protos = self.converter.to_proto_values(values, ValueType.BYTES)
        assert len(protos) == 2
        assert protos[0].bytes_val == b"hello"

    def test_list_int_conversion(self):
        """Test integer list conversion."""
        values = [[1, 2, 3], [4, 5, 6]]
        protos = self.converter.to_proto_values(values, ValueType.INT64_LIST)
        assert len(protos) == 2
        assert list(protos[0].int64_list_val.val) == [1, 2, 3]

    def test_list_string_conversion(self):
        """Test string list conversion."""
        values = [["a", "b"], ["c", "d", "e"]]
        protos = self.converter.to_proto_values(values, ValueType.STRING_LIST)
        assert len(protos) == 2
        assert list(protos[0].string_list_val.val) == ["a", "b"]

    def test_set_int_conversion(self):
        """Test integer set conversion."""
        values = [{1, 2, 3}, {4, 5}]
        protos = self.converter.to_proto_values(values, ValueType.INT64_SET)
        assert len(protos) == 2
        # Sets may not preserve order
        assert set(protos[0].int64_set_val.val) == {1, 2, 3}

    def test_map_conversion(self):
        """Test map conversion."""
        values = [{"key1": "value1", "key2": "value2"}]
        protos = self.converter.to_proto_values(values, ValueType.MAP)
        assert len(protos) == 1
        assert protos[0].map_val.val["key1"].string_val == "value1"

    def test_from_proto_value_scalar(self):
        """Test converting proto value back to Python."""
        proto = ProtoValue(int64_val=42)
        result = self.converter.from_proto_value(proto)
        assert result == 42

    def test_from_proto_value_string(self):
        """Test converting string proto value back to Python."""
        proto = ProtoValue(string_val="hello")
        result = self.converter.from_proto_value(proto)
        assert result == "hello"

    def test_from_proto_value_null(self):
        """Test converting null proto value back to Python."""
        proto = ProtoValue()
        result = self.converter.from_proto_value(proto)
        assert result is None


class TestTypeHandlers:
    """Tests for individual type handlers."""

    def test_scalar_handler_can_handle(self):
        """Test scalar handler type detection."""
        handler = ScalarTypeHandler()
        assert handler.can_handle(ValueType.INT64)
        assert handler.can_handle(ValueType.STRING)
        assert handler.can_handle(ValueType.UNIX_TIMESTAMP)
        assert not handler.can_handle(ValueType.INT64_LIST)

    def test_list_handler_can_handle(self):
        """Test list handler type detection."""
        handler = ListTypeHandler()
        assert handler.can_handle(ValueType.INT64_LIST)
        assert handler.can_handle(ValueType.STRING_LIST)
        assert not handler.can_handle(ValueType.INT64)
        assert not handler.can_handle(ValueType.INT64_SET)

    def test_set_handler_can_handle(self):
        """Test set handler type detection."""
        handler = SetTypeHandler()
        assert handler.can_handle(ValueType.INT64_SET)
        assert handler.can_handle(ValueType.STRING_SET)
        assert not handler.can_handle(ValueType.INT64)
        assert not handler.can_handle(ValueType.INT64_LIST)

    def test_map_handler_can_handle(self):
        """Test map handler type detection."""
        handler = MapTypeHandler()
        assert handler.can_handle(ValueType.MAP)
        assert handler.can_handle(ValueType.MAP_LIST)
        assert not handler.can_handle(ValueType.INT64)


class TestEntityConverter:
    """Tests for Entity converter."""

    def setup_method(self):
        """Reset registration before each test."""
        reset_registration()

    def test_entity_to_proto(self):
        """Test converting Entity to proto."""
        converter = EntityConverter()
        entity = Entity(
            name="user",
            join_keys=["user_id"],
            value_type=ValueType.INT64,
            description="User entity",
            tags={"team": "ml"},
            owner="ml-team@example.com",
        )
        proto = converter.to_proto(entity)
        assert proto.spec.name == "user"
        assert proto.spec.join_key == "user_id"
        assert proto.spec.value_type == ValueType.INT64.value
        assert proto.spec.description == "User entity"
        assert dict(proto.spec.tags) == {"team": "ml"}

    def test_entity_from_proto(self):
        """Test converting proto to Entity."""
        converter = EntityConverter()
        entity = Entity(
            name="product",
            join_keys=["product_id"],
            value_type=ValueType.STRING,
        )
        proto = converter.to_proto(entity)
        restored = converter.from_proto(proto)
        assert restored.name == "product"
        assert restored.join_key == "product_id"
        assert restored.value_type == ValueType.STRING

    def test_entity_roundtrip(self):
        """Test entity conversion roundtrip."""
        converter = EntityConverter()
        entity = Entity(
            name="customer",
            join_keys=["customer_id"],
            value_type=ValueType.INT64,
            description="Customer entity",
            tags={"env": "prod"},
            owner="data-team@example.com",
        )
        entity.created_timestamp = datetime.now(tz=timezone.utc)
        entity.last_updated_timestamp = datetime.now(tz=timezone.utc)

        proto = converter.to_proto(entity)
        restored = converter.from_proto(proto)

        assert entity == restored


class TestModuleFunctions:
    """Tests for module-level convenience functions."""

    def test_python_values_to_proto_values(self):
        """Test the convenience function for value conversion."""
        protos = python_values_to_proto_values([1, 2, 3], ValueType.INT64)
        assert len(protos) == 3
        assert protos[0].int64_val == 1

    def test_proto_value_to_python(self):
        """Test the convenience function for proto to python."""
        proto = ProtoValue(string_val="test")
        result = proto_value_to_python(proto)
        assert result == "test"

    def test_get_value_converter(self):
        """Test getting the global value converter."""
        converter = get_value_converter()
        assert isinstance(converter, ValueTypeConverter)
        # Should return the same instance
        converter2 = get_value_converter()
        assert converter is converter2


class TestRegistration:
    """Tests for converter registration."""

    def setup_method(self):
        """Reset registration before each test."""
        reset_registration()

    def teardown_method(self):
        """Clean up after each test."""
        reset_registration()

    def test_register_all_converters(self):
        """Test registering all converters."""
        register_all_converters()
        registry = get_registry()
        assert registry.has_converter(Entity)

    def test_register_idempotent(self):
        """Test that registration is idempotent."""
        register_all_converters()
        register_all_converters()  # Should not raise
        registry = get_registry()
        assert registry.has_converter(Entity)


class TestErrorHandling:
    """Tests for error handling."""

    def test_unsupported_type_error(self):
        """Test UnsupportedTypeError attributes."""
        error = UnsupportedTypeError(
            value="test",
            expected_types=[int, float],
            context="test conversion",
        )
        assert error.actual_type == str
        assert "int" in str(error)
        assert "float" in str(error)

    def test_validation_error(self):
        """Test ValidationError attributes."""
        error = ValidationError(
            message="Invalid value",
            field_name="test_field",
            value=123,
        )
        assert "test_field" in str(error)

    def test_proto_conversion_error_with_cause(self):
        """Test ProtoConversionError with cause."""
        cause = ValueError("Original error")
        error = ProtoConversionError("Conversion failed", cause=cause)
        assert error.cause is cause
        assert "Original error" in str(error)


class TestBackwardCompatibility:
    """Tests for backward compatibility with type_map.py."""

    def test_compat_python_values_to_proto_values(self):
        """Test compat layer for python_values_to_proto_values."""
        from feast.proto_conversion.compat import (
            python_values_to_proto_values as compat_func,
        )

        # Should work the same as the original
        protos = compat_func([1, 2, 3], ValueType.INT64)
        assert len(protos) == 3
        assert protos[0].int64_val == 1

    def test_compat_feast_value_type_to_python_type(self):
        """Test compat layer for feast_value_type_to_python_type."""
        from feast.proto_conversion.compat import (
            feast_value_type_to_python_type as compat_func,
        )

        proto = ProtoValue(int64_val=42)
        result = compat_func(proto)
        assert result == 42


class TestNumpyIntegration:
    """Tests for numpy type integration."""

    def test_numpy_int64_conversion(self):
        """Test numpy int64 conversion."""
        converter = ValueTypeConverter()
        values = [np.int64(1), np.int64(2), np.int64(3)]
        protos = converter.to_proto_values(values, ValueType.INT64)
        assert len(protos) == 3
        assert protos[0].int64_val == 1

    def test_numpy_float64_conversion(self):
        """Test numpy float64 conversion."""
        converter = ValueTypeConverter()
        values = [np.float64(1.5), np.float64(2.5)]
        protos = converter.to_proto_values(values, ValueType.DOUBLE)
        assert len(protos) == 2
        assert protos[0].double_val == 1.5

    def test_numpy_bool_conversion(self):
        """Test numpy bool conversion."""
        converter = ValueTypeConverter()
        values = [np.bool_(True), np.bool_(False)]
        protos = converter.to_proto_values(values, ValueType.BOOL)
        assert len(protos) == 2
        assert protos[0].bool_val is True
        assert protos[1].bool_val is False

    def test_numpy_array_list_conversion(self):
        """Test numpy array as list conversion."""
        converter = ValueTypeConverter()
        values = [np.array([1, 2, 3]), np.array([4, 5, 6])]
        protos = converter.to_proto_values(values, ValueType.INT64_LIST)
        assert len(protos) == 2
        assert list(protos[0].int64_list_val.val) == [1, 2, 3]


class TestTimestampConversion:
    """Tests for timestamp conversion."""

    def test_datetime_to_proto(self):
        """Test datetime conversion to proto."""
        converter = ValueTypeConverter()
        now = datetime.now(tz=timezone.utc)
        values = [now]
        protos = converter.to_proto_values(values, ValueType.UNIX_TIMESTAMP)
        assert len(protos) == 1
        assert protos[0].unix_timestamp_val == int(now.timestamp())

    def test_timestamp_from_proto(self):
        """Test timestamp conversion from proto."""
        converter = ValueTypeConverter()
        now = datetime.now(tz=timezone.utc)
        ts = int(now.timestamp())
        proto = ProtoValue(unix_timestamp_val=ts)
        result = converter.from_proto_value(proto)
        # The result should be a datetime object
        assert isinstance(result, datetime)
        # Compare timestamps (datetime objects may differ slightly)
        assert abs(result.timestamp() - now.timestamp()) < 1

    def test_timestamp_list_conversion(self):
        """Test timestamp list conversion."""
        converter = ValueTypeConverter()
        now = datetime.now(tz=timezone.utc)
        values = [[now, now]]
        protos = converter.to_proto_values(values, ValueType.UNIX_TIMESTAMP_LIST)
        assert len(protos) == 1
        assert len(protos[0].unix_timestamp_list_val.val) == 2
