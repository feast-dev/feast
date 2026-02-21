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
Consolidated ValueTypeConverter for proto value conversions.

This module consolidates the complex value conversion logic from type_map.py
into a cleaner, more maintainable architecture. It provides:

1. Type-specific handlers instead of 25+ nested branches
2. Centralized type mapping dictionaries
3. Consistent error handling
4. Full backward compatibility with existing APIs

The converter handles:
- Scalar types (int, float, string, bytes, bool, timestamp)
- List types (repeated values)
- Set types (unique values stored as repeated)
- Map types (string -> value dictionaries)
- Nested structures (map of maps, list of maps)
"""

import decimal
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Type, Union, cast

import numpy as np
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp

from feast.proto_conversion.errors import (
    SerializationError,
    TypeMappingError,
    UnsupportedTypeError,
)
from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BoolSet,
    BytesList,
    BytesSet,
    DoubleList,
    DoubleSet,
    FloatList,
    FloatSet,
    Int32List,
    Int32Set,
    Int64List,
    Int64Set,
    Map,
    MapList,
    StringList,
    StringSet,
)
from feast.protos.feast.types.Value_pb2 import Value as ProtoValue
from feast.value_type import ListType, SetType, ValueType

logger = logging.getLogger(__name__)

# null timestamps get converted to -9223372036854775808
NULL_TIMESTAMP_INT_VALUE: int = np.datetime64("NaT").astype(int)


class TypeHandler(ABC):
    """
    Abstract base class for handling specific value type conversions.

    Each handler is responsible for a category of value types (scalar, list, set, map).
    This replaces the nested if/elif chains with polymorphic dispatch.
    """

    @abstractmethod
    def can_handle(self, value_type: ValueType) -> bool:
        """Check if this handler can convert the given value type."""
        ...

    @abstractmethod
    def to_proto_values(
        self, value_type: ValueType, values: List[Any]
    ) -> List[ProtoValue]:
        """Convert Python values to proto values."""
        ...

    @abstractmethod
    def from_proto_value(self, proto: ProtoValue) -> Any:
        """Convert a proto value to Python value."""
        ...


class ScalarTypeHandler(TypeHandler):
    """Handler for scalar value types (int, float, string, bytes, bool, timestamp)."""

    # Mapping: ValueType -> (proto_field_name, conversion_function, valid_python_types)
    SCALAR_TYPE_MAP: Dict[ValueType, tuple[str, Callable, Optional[Set[Type]]]] = {
        ValueType.INT32: ("int32_val", lambda x: int(x), None),
        ValueType.INT64: (
            "int64_val",
            lambda x: (
                int(x.timestamp())
                if isinstance(x, pd._libs.tslibs.timestamps.Timestamp)
                else int(x)
            ),
            None,
        ),
        ValueType.FLOAT: ("float_val", lambda x: float(x), None),
        ValueType.DOUBLE: (
            "double_val",
            lambda x: x,
            {float, np.float64, int, np.int_, decimal.Decimal},
        ),
        ValueType.STRING: ("string_val", lambda x: str(x), None),
        ValueType.BYTES: ("bytes_val", lambda x: x, {bytes}),
        ValueType.IMAGE_BYTES: ("bytes_val", lambda x: x, {bytes}),
        ValueType.BOOL: ("bool_val", lambda x: x, {bool, np.bool_, int, np.int_}),
    }

    def can_handle(self, value_type: ValueType) -> bool:
        return (
            value_type in self.SCALAR_TYPE_MAP or value_type == ValueType.UNIX_TIMESTAMP
        )

    def to_proto_values(
        self, value_type: ValueType, values: List[Any]
    ) -> List[ProtoValue]:
        sample = _first_non_empty(values)

        if sample is None:
            return [ProtoValue()] * len(values)

        if value_type == ValueType.UNIX_TIMESTAMP:
            return self._convert_timestamps(values)

        field_name, func, valid_types = self.SCALAR_TYPE_MAP[value_type]

        if valid_types:
            self._validate_scalar_type(sample, value_type, valid_types)

        if value_type == ValueType.BOOL:
            return self._convert_bools(values, field_name, func)

        return self._convert_scalars(values, field_name, func)

    def from_proto_value(self, proto: ProtoValue) -> Any:
        val_attr = proto.WhichOneof("val")
        if val_attr is None:
            return None

        val = getattr(proto, val_attr)

        if val_attr == "unix_timestamp_val":
            return (
                datetime.fromtimestamp(val, tz=timezone.utc)
                if val != NULL_TIMESTAMP_INT_VALUE
                else None
            )

        return val

    def _convert_timestamps(self, values: List[Any]) -> List[ProtoValue]:
        int_timestamps = _python_datetime_to_int_timestamp(values)
        return [ProtoValue(unix_timestamp_val=ts) for ts in int_timestamps]  # type: ignore

    def _convert_bools(
        self, values: List[Any], field_name: str, func: Callable
    ) -> List[ProtoValue]:
        result = []
        for value in values:
            if not pd.isnull(value):
                converted = bool(value) if type(value) is np.bool_ else value
                result.append(ProtoValue(**{field_name: func(converted)}))
            else:
                result.append(ProtoValue())
        return result

    def _convert_scalars(
        self, values: List[Any], field_name: str, func: Callable
    ) -> List[ProtoValue]:
        result = []
        for value in values:
            if isinstance(value, ProtoValue):
                result.append(value)
            elif not pd.isnull(value):
                result.append(ProtoValue(**{field_name: func(value)}))
            else:
                result.append(ProtoValue())
        return result

    def _validate_scalar_type(
        self, sample: Any, value_type: ValueType, valid_types: Set[Type]
    ) -> None:
        if (sample == 0 or sample == 0.0) and value_type != ValueType.BOOL:
            # Numpy converts 0 to int, but column may be float
            allowed_types = {np.int64, int, np.float64, float, decimal.Decimal}
            if type(sample) not in allowed_types:
                raise TypeMappingError(type(sample), "proto scalar")
        elif type(sample) not in valid_types:
            raise TypeMappingError(type(sample), "proto scalar")


class ListTypeHandler(TypeHandler):
    """Handler for list value types (repeated values)."""

    # Mapping: ValueType -> (ProtoListType, proto_field_name, valid_element_types)
    LIST_TYPE_MAP: Dict[ValueType, tuple[ListType, str, List[Type]]] = {
        ValueType.FLOAT_LIST: (
            FloatList,
            "float_list_val",
            [np.float32, np.float64, float],
        ),
        ValueType.DOUBLE_LIST: (
            DoubleList,
            "double_list_val",
            [np.float64, np.float32, float],
        ),
        ValueType.INT32_LIST: (Int32List, "int32_list_val", [np.int64, np.int32, int]),
        ValueType.INT64_LIST: (Int64List, "int64_list_val", [np.int64, np.int32, int]),
        ValueType.UNIX_TIMESTAMP_LIST: (
            Int64List,
            "int64_list_val",
            [np.datetime64, np.int64, np.int32, int, datetime, Timestamp],
        ),
        ValueType.STRING_LIST: (StringList, "string_list_val", [np.str_, str]),
        ValueType.BOOL_LIST: (BoolList, "bool_list_val", [np.bool_, bool]),
        ValueType.BYTES_LIST: (BytesList, "bytes_list_val", [np.bytes_, bytes]),
    }

    def can_handle(self, value_type: ValueType) -> bool:
        return "list" in value_type.name.lower() and value_type in self.LIST_TYPE_MAP

    def to_proto_values(
        self, value_type: ValueType, values: List[Any]
    ) -> List[ProtoValue]:
        if value_type not in self.LIST_TYPE_MAP:
            return []

        proto_type, field_name, valid_types = self.LIST_TYPE_MAP[value_type]
        sample = _first_non_empty(values)

        # Handle bytes that contain JSON-encoded arrays
        if isinstance(sample, (bytes, bytearray)):
            return self._convert_json_bytes(
                values, value_type, proto_type, field_name, valid_types
            )

        # Validate element types
        if sample is not None:
            self._validate_list_elements(sample, value_type, valid_types)

        # Special handling for timestamp lists
        if value_type == ValueType.UNIX_TIMESTAMP_LIST:
            return self._convert_timestamp_list(values)

        # Special handling for bool lists (np.bool_ conversion)
        if value_type == ValueType.BOOL_LIST:
            return self._convert_bool_list(values, proto_type, field_name)

        # Standard list conversion
        return self._convert_standard_list(values, proto_type, field_name)

    def from_proto_value(self, proto: ProtoValue) -> Any:
        val_attr = proto.WhichOneof("val")
        if val_attr is None:
            return None

        # Only handle list types
        if not val_attr.endswith("_list_val"):
            return None

        val = getattr(proto, val_attr)

        # Extract list values
        if hasattr(val, "val"):
            val = list(val.val)

        if val_attr == "unix_timestamp_list_val":
            return [
                datetime.fromtimestamp(v, tz=timezone.utc)
                if v != NULL_TIMESTAMP_INT_VALUE
                else None
                for v in val
            ]

        return val

    def _convert_json_bytes(
        self,
        values: List[Any],
        value_type: ValueType,
        proto_type: ListType,
        field_name: str,
        valid_types: List[Type],
    ) -> List[ProtoValue]:
        if value_type == ValueType.BYTES_LIST:
            raise UnsupportedTypeError(
                values[0],
                context="Bytes of an array containing elements of bytes not supported",
            )

        sample = _first_non_empty(values)
        json_sample = json.loads(sample)

        if isinstance(json_sample, list):
            json_values = [
                json.loads(value) if value is not None else None for value in values
            ]
            if value_type == ValueType.BOOL_LIST:
                json_values = [
                    [bool(item) for item in list_item] if list_item else None
                    for list_item in json_values
                ]
            return [
                ProtoValue(**{field_name: proto_type(val=v)})  # type: ignore
                if v is not None
                else ProtoValue()
                for v in json_values
            ]

        raise UnsupportedTypeError(sample, expected_types=[valid_types[0]])

    def _validate_list_elements(
        self, sample: List, value_type: ValueType, valid_types: List[Type]
    ) -> None:
        if not all(type(item) in valid_types for item in sample):
            for item in sample:
                if type(item) not in valid_types:
                    if value_type in [ValueType.INT32_LIST, ValueType.INT64_LIST]:
                        if not any(np.isnan(x) for x in sample):
                            logger.error(
                                "Array of Int32 or Int64 type has NULL values. "
                                "to_numpy() upcasts to Float64 automatically."
                            )
                    raise UnsupportedTypeError(item, expected_types=valid_types)

    def _convert_timestamp_list(self, values: List[Any]) -> List[ProtoValue]:
        result = []
        for value in values:
            if value is not None:
                result.append(
                    ProtoValue(
                        unix_timestamp_list_val=Int64List(
                            val=_python_datetime_to_int_timestamp(value)  # type: ignore
                        )
                    )
                )
            else:
                result.append(ProtoValue())
        return result

    def _convert_bool_list(
        self, values: List[Any], proto_type: ListType, field_name: str
    ) -> List[ProtoValue]:
        result = []
        for value in values:
            if value is not None:
                result.append(
                    ProtoValue(**{field_name: proto_type(val=[bool(e) for e in value])})  # type: ignore
                )
            else:
                result.append(ProtoValue())
        return result

    def _convert_standard_list(
        self, values: List[Any], proto_type: ListType, field_name: str
    ) -> List[ProtoValue]:
        return [
            ProtoValue(**{field_name: proto_type(val=value)})  # type: ignore
            if value is not None
            else ProtoValue()
            for value in values
        ]


class SetTypeHandler(TypeHandler):
    """Handler for set value types (unique values stored as repeated)."""

    # Mapping: ValueType -> (ProtoSetType, proto_field_name, valid_element_types)
    SET_TYPE_MAP: Dict[ValueType, tuple[SetType, str, List[Type]]] = {
        ValueType.FLOAT_SET: (
            FloatSet,
            "float_set_val",
            [np.float32, np.float64, float],
        ),
        ValueType.DOUBLE_SET: (
            DoubleSet,
            "double_set_val",
            [np.float64, np.float32, float],
        ),
        ValueType.INT32_SET: (Int32Set, "int32_set_val", [np.int64, np.int32, int]),
        ValueType.INT64_SET: (Int64Set, "int64_set_val", [np.int64, np.int32, int]),
        ValueType.UNIX_TIMESTAMP_SET: (
            Int64Set,
            "unix_timestamp_set_val",
            [np.datetime64, np.int64, np.int32, int, datetime, Timestamp],
        ),
        ValueType.STRING_SET: (StringSet, "string_set_val", [np.str_, str]),
        ValueType.BOOL_SET: (BoolSet, "bool_set_val", [np.bool_, bool]),
        ValueType.BYTES_SET: (BytesSet, "bytes_set_val", [np.bytes_, bytes]),
    }

    def can_handle(self, value_type: ValueType) -> bool:
        return "set" in value_type.name.lower() and value_type in self.SET_TYPE_MAP

    def to_proto_values(
        self, value_type: ValueType, values: List[Any]
    ) -> List[ProtoValue]:
        if value_type not in self.SET_TYPE_MAP:
            return []

        proto_type, field_name, valid_types = self.SET_TYPE_MAP[value_type]

        # Convert sets to lists for proto representation
        converted_values = [self._set_to_list(v) for v in values]
        sample = _first_non_empty(converted_values)

        # Handle bytes that contain JSON-encoded sets
        if isinstance(sample, (bytes, bytearray)):
            return self._convert_json_bytes(
                converted_values, value_type, proto_type, field_name, valid_types
            )

        # Validate element types
        if sample is not None:
            self._validate_set_elements(sample, value_type, valid_types)

        # Special handling for timestamp sets
        if value_type == ValueType.UNIX_TIMESTAMP_SET:
            return self._convert_timestamp_set(converted_values)

        # Special handling for bool sets
        if value_type == ValueType.BOOL_SET:
            return self._convert_bool_set(converted_values, proto_type, field_name)

        # Standard set conversion
        return self._convert_standard_set(converted_values, proto_type, field_name)

    def from_proto_value(self, proto: ProtoValue) -> Any:
        val_attr = proto.WhichOneof("val")
        if val_attr is None:
            return None

        # Only handle set types
        if not val_attr.endswith("_set_val"):
            return None

        val = getattr(proto, val_attr)

        # Extract set values
        if hasattr(val, "val"):
            val = list(val.val)

        if val_attr == "unix_timestamp_set_val":
            return set(
                datetime.fromtimestamp(v, tz=timezone.utc)
                if v != NULL_TIMESTAMP_INT_VALUE
                else None
                for v in val
            )

        return set(val)

    def _set_to_list(self, value: Any) -> Optional[List]:
        """Convert set-like value to list for proto representation."""
        if value is None:
            return None
        if isinstance(value, set):
            return list(value)
        if isinstance(value, (list, tuple, np.ndarray)):
            return list(set(value))
        return value

    def _convert_json_bytes(
        self,
        values: List[Any],
        value_type: ValueType,
        proto_type: SetType,
        field_name: str,
        valid_types: List[Type],
    ) -> List[ProtoValue]:
        sample = _first_non_empty(values)

        if value_type == ValueType.BYTES_SET:
            raise UnsupportedTypeError(
                sample,
                context="Bytes of a set containing elements of bytes not supported",
            )

        json_sample = json.loads(sample)

        if isinstance(json_sample, list):
            json_values = [
                json.loads(value) if value is not None else None for value in values
            ]
            if value_type == ValueType.BOOL_SET:
                json_values = [
                    [bool(item) for item in list_item] if list_item else None
                    for list_item in json_values
                ]
            return [
                ProtoValue(**{field_name: proto_type(val=v)})  # type: ignore
                if v is not None
                else ProtoValue()
                for v in json_values
            ]

        raise UnsupportedTypeError(sample, expected_types=[valid_types[0]])

    def _validate_set_elements(
        self, sample: List, value_type: ValueType, valid_types: List[Type]
    ) -> None:
        if not all(type(item) in valid_types for item in sample):
            for item in sample:
                if type(item) not in valid_types:
                    if value_type in [ValueType.INT32_SET, ValueType.INT64_SET]:
                        if not any(np.isnan(x) for x in sample):
                            logger.error("Set of Int32 or Int64 type has NULL values.")
                    raise UnsupportedTypeError(item, expected_types=valid_types)

    def _convert_timestamp_set(self, values: List[Any]) -> List[ProtoValue]:
        result = []
        for value in values:
            if value is not None:
                result.append(
                    ProtoValue(
                        unix_timestamp_set_val=Int64Set(
                            val=_python_datetime_to_int_timestamp(value)  # type: ignore
                        )
                    )
                )
            else:
                result.append(ProtoValue())
        return result

    def _convert_bool_set(
        self, values: List[Any], proto_type: SetType, field_name: str
    ) -> List[ProtoValue]:
        result = []
        for value in values:
            if value is not None:
                result.append(
                    ProtoValue(**{field_name: proto_type(val=[bool(e) for e in value])})  # type: ignore
                )
            else:
                result.append(ProtoValue())
        return result

    def _convert_standard_set(
        self, values: List[Any], proto_type: SetType, field_name: str
    ) -> List[ProtoValue]:
        return [
            ProtoValue(**{field_name: proto_type(val=value)})  # type: ignore
            if value is not None
            else ProtoValue()
            for value in values
        ]


class MapTypeHandler(TypeHandler):
    """Handler for map value types (string -> value dictionaries)."""

    def can_handle(self, value_type: ValueType) -> bool:
        return value_type in (ValueType.MAP, ValueType.MAP_LIST)

    def to_proto_values(
        self, value_type: ValueType, values: List[Any]
    ) -> List[ProtoValue]:
        if value_type == ValueType.MAP:
            return [
                ProtoValue(map_val=self._dict_to_map_proto(value))
                if value is not None
                else ProtoValue()
                for value in values
            ]

        if value_type == ValueType.MAP_LIST:
            return [
                ProtoValue(map_list_val=self._list_to_map_list_proto(value))
                if value is not None
                else ProtoValue()
                for value in values
            ]

        return []

    def from_proto_value(self, proto: ProtoValue) -> Any:
        val_attr = proto.WhichOneof("val")
        if val_attr is None:
            return None

        val = getattr(proto, val_attr)

        if val_attr == "map_val":
            return self._map_proto_to_dict(val)

        if val_attr == "map_list_val":
            return self._map_list_proto_to_list(val)

        return None

    def _dict_to_map_proto(self, python_dict: Dict[str, Any]) -> Map:
        """Convert a Python dictionary to a Map proto message."""
        map_proto = Map()

        for key, value in python_dict.items():
            if value is None:
                map_proto.val[key].CopyFrom(ProtoValue())
                continue

            if isinstance(value, dict):
                nested_map = self._dict_to_map_proto(value)
                map_proto.val[key].CopyFrom(ProtoValue(map_val=nested_map))
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                map_list = self._list_to_map_list_proto(value)
                map_proto.val[key].CopyFrom(ProtoValue(map_list_val=map_list))
            else:
                # Use the main converter for scalar/list values
                proto_values = python_values_to_proto_values([value], ValueType.UNKNOWN)
                map_proto.val[key].CopyFrom(proto_values[0])

        return map_proto

    def _list_to_map_list_proto(self, python_list: List[Dict[str, Any]]) -> MapList:
        """Convert a Python list of dictionaries to a MapList proto message."""
        map_list_proto = MapList()

        for item in python_list:
            if isinstance(item, dict):
                map_proto = self._dict_to_map_proto(item)
                map_list_proto.val.append(map_proto)
            else:
                raise SerializationError(
                    item,
                    context=f"MapList can only contain dictionaries, got {type(item)}",
                )

        return map_list_proto

    def _map_proto_to_dict(self, map_message: Map) -> Dict[str, Any]:
        """Convert a Map proto message to a Python dictionary."""
        result = {}
        for key, value in map_message.val.items():
            result[key] = proto_value_to_python(value)
        return result

    def _map_list_proto_to_list(
        self, map_list_message: MapList
    ) -> List[Dict[str, Any]]:
        """Convert a MapList proto message to a Python list of dictionaries."""
        return [self._map_proto_to_dict(map_item) for map_item in map_list_message.val]


class ValueTypeConverter:
    """
    Centralized converter for Python values to/from proto Value messages.

    This class consolidates the complex conversion logic from type_map.py
    into a cleaner architecture using type-specific handlers.

    Example:
        >>> converter = ValueTypeConverter()
        >>> proto_values = converter.to_proto_values([1, 2, 3], ValueType.INT64)
        >>> python_values = [converter.from_proto_value(p) for p in proto_values]
    """

    def __init__(self):
        self._handlers: List[TypeHandler] = [
            MapTypeHandler(),
            ListTypeHandler(),
            SetTypeHandler(),
            ScalarTypeHandler(),
        ]

    def to_proto_values(
        self, values: List[Any], value_type: ValueType
    ) -> List[ProtoValue]:
        """
        Convert Python values to proto values.

        Args:
            values: List of Python values to convert.
            value_type: The target Feast ValueType.

        Returns:
            List of ProtoValue messages.

        Raises:
            UnsupportedTypeError: If the value type is not supported.
        """
        for handler in self._handlers:
            if handler.can_handle(value_type):
                return handler.to_proto_values(value_type, values)

        raise UnsupportedTypeError(
            values[0] if values else None,
            context=f"No handler for value type {value_type}",
        )

    def from_proto_value(self, proto: ProtoValue) -> Any:
        """
        Convert a proto value to its Python representation.

        Args:
            proto: The ProtoValue message to convert.

        Returns:
            The Python value.
        """
        val_attr = proto.WhichOneof("val")
        if val_attr is None:
            return None

        # Try each handler to find one that can process this proto
        for handler in self._handlers:
            try:
                result = handler.from_proto_value(proto)
                if result is not None:
                    return result
            except Exception:
                continue

        # Fallback: return the raw value
        return getattr(proto, val_attr)

    def get_handler(self, value_type: ValueType) -> Optional[TypeHandler]:
        """Get the handler for a specific value type."""
        for handler in self._handlers:
            if handler.can_handle(value_type):
                return handler
        return None


# Singleton instance for convenience
_converter_instance: Optional[ValueTypeConverter] = None


def get_value_converter() -> ValueTypeConverter:
    """Get the global ValueTypeConverter instance."""
    global _converter_instance
    if _converter_instance is None:
        _converter_instance = ValueTypeConverter()
    return _converter_instance


# Helper functions (maintaining backward compatibility with type_map.py)


def _first_non_empty(values: List[Any]) -> Any:
    """Get the first non-empty value from a list."""
    return next(filter(_non_empty_value, values), None)


def _non_empty_value(value: Any) -> bool:
    """Check if a value is non-empty (not None, not NaN, not empty)."""
    if value is None:
        return False
    if isinstance(value, float) and np.isnan(value):
        return False
    if hasattr(value, "__len__") and len(value) == 0:
        return False
    return True


def _python_datetime_to_int_timestamp(
    values: Sequence[Any],
) -> Sequence[Union[int, np.int_]]:
    """Convert datetime values to integer timestamps."""
    # Fast path for Numpy array
    if isinstance(values, np.ndarray) and isinstance(values.dtype, np.datetime64):
        if values.ndim != 1:
            raise ValueError("Only 1 dimensional arrays are supported.")
        return cast(Sequence[np.int_], values.astype("datetime64[s]").astype(np.int_))

    int_timestamps = []
    for value in values:
        if isinstance(value, datetime):
            int_timestamps.append(int(value.timestamp()))
        elif isinstance(value, Timestamp):
            int_timestamps.append(int(value.ToSeconds()))
        elif isinstance(value, np.datetime64):
            int_timestamps.append(value.astype("datetime64[s]").astype(np.int_))  # type: ignore[attr-defined]
        elif isinstance(value, type(np.nan)):
            int_timestamps.append(NULL_TIMESTAMP_INT_VALUE)
        else:
            int_timestamps.append(int(value))
    return int_timestamps


def proto_value_to_python(proto: ProtoValue) -> Any:
    """
    Convert a ProtoValue to its Python representation.

    This is the main entry point for proto-to-Python conversion,
    delegating to the global converter instance.

    Args:
        proto: The ProtoValue message to convert.

    Returns:
        The Python value.
    """
    return get_value_converter().from_proto_value(proto)


def python_values_to_proto_values(
    values: List[Any], feature_type: ValueType = ValueType.UNKNOWN
) -> List[ProtoValue]:
    """
    Convert Python values to proto values.

    This is the main entry point for Python-to-proto conversion,
    maintaining backward compatibility with type_map.py.

    Args:
        values: List of Python values to convert.
        feature_type: The target Feast ValueType. If UNKNOWN, the type
                      will be inferred from the values.

    Returns:
        List of ProtoValue messages.

    Raises:
        TypeError: If the value type cannot be inferred.
    """
    from feast.type_map import python_type_to_feast_value_type

    value_type = feature_type
    sample = _first_non_empty(values)

    if sample is not None and feature_type == ValueType.UNKNOWN:
        if isinstance(sample, (list, np.ndarray)):
            value_type = (
                feature_type
                if len(sample) == 0
                else python_type_to_feast_value_type("", sample)
            )
        else:
            value_type = python_type_to_feast_value_type("", sample)

    if value_type == ValueType.UNKNOWN:
        raise TypeError("Couldn't infer value type from empty value")

    converter = get_value_converter()
    proto_values = converter.to_proto_values(values, value_type)

    if len(proto_values) != len(values):
        raise ValueError(
            f"Number of proto values {len(proto_values)} does not match "
            f"number of values {len(values)}"
        )

    return proto_values
