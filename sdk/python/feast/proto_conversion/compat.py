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
Backward compatibility layer for proto conversion.

This module provides compatibility shims that allow gradual migration from
the old type_map.py functions to the new proto_conversion system.

The approach is:
1. Keep the existing type_map.py functions unchanged (they still work)
2. Provide drop-in replacements that use the new converters
3. Allow users to opt-in to the new system when ready
4. Eventually deprecate the old functions

Usage:
    # Existing code continues to work unchanged:
    from feast.type_map import python_values_to_proto_values

    # New code can use the new converters directly:
    from feast.proto_conversion import get_value_converter
    converter = get_value_converter()
    proto_values = converter.to_proto_values(values, value_type)

    # Or use the compatibility layer for gradual migration:
    from feast.proto_conversion.compat import python_values_to_proto_values
    proto_values = python_values_to_proto_values(values, value_type)
"""

from typing import Any, Dict, List, Union

import pyarrow

from feast.proto_conversion.converters.value_converter import (
    proto_value_to_python as _proto_value_to_python,
)
from feast.proto_conversion.converters.value_converter import (
    python_values_to_proto_values as _python_values_to_proto_values,
)
from feast.protos.feast.types.Value_pb2 import Value as ProtoValue
from feast.value_type import ValueType


def python_values_to_proto_values(
    values: List[Any], feature_type: ValueType = ValueType.UNKNOWN
) -> List[ProtoValue]:
    """
    Convert Python values to proto values using the new converter system.

    This is a drop-in replacement for feast.type_map.python_values_to_proto_values
    that uses the new, cleaner converter architecture.

    Args:
        values: List of Python values to convert.
        feature_type: The target Feast ValueType. If UNKNOWN, the type
                      will be inferred from the values.

    Returns:
        List of ProtoValue messages.

    Raises:
        TypeError: If the value type cannot be inferred.
    """
    return _python_values_to_proto_values(values, feature_type)


def feast_value_type_to_python_type(field_value_proto: ProtoValue) -> Any:
    """
    Convert a ProtoValue to its Python representation.

    This is a drop-in replacement for feast.type_map.feast_value_type_to_python_type
    that uses the new converter architecture.

    Args:
        field_value_proto: The ProtoValue message to convert.

    Returns:
        The Python value.
    """
    return _proto_value_to_python(field_value_proto)


def convert_arrow_to_proto(
    table: Union[pyarrow.Table, pyarrow.RecordBatch],
    feature_view: Any,
    join_keys: Dict[str, ValueType],
):
    """
    Convert an Arrow table to proto representation.

    This is a drop-in replacement for feast.utils._convert_arrow_to_proto
    that uses the new DataFrameProtoConverter.

    Args:
        table: The Arrow table or record batch to convert.
        feature_view: The feature view defining the schema.
        join_keys: Dictionary mapping join key names to their value types.

    Returns:
        List of tuples containing (entity_key, features, event_ts, created_ts)
        for each row in the table.
    """
    from feast.proto_conversion.converters.dataframe_converter import (
        convert_arrow_to_proto as _convert_arrow_to_proto,
    )

    return _convert_arrow_to_proto(table, feature_view, join_keys)


# Mapping of old function names to new implementations
# This can be used by tools to automate migration
COMPAT_MAPPING = {
    "feast.type_map.python_values_to_proto_values": python_values_to_proto_values,
    "feast.type_map.feast_value_type_to_python_type": feast_value_type_to_python_type,
    "feast.utils._convert_arrow_to_proto": convert_arrow_to_proto,
}


def get_compat_function(old_name: str):
    """
    Get the new implementation for an old function name.

    Args:
        old_name: The fully qualified name of the old function.

    Returns:
        The new implementation function.

    Raises:
        KeyError: If no compatibility mapping exists.
    """
    return COMPAT_MAPPING[old_name]
