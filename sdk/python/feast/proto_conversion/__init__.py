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
Feast Protobuf Conversion System.

This module provides a centralized, type-safe system for converting between
Python objects and their protobuf representations. It replaces the scattered
conversion logic throughout the codebase with a unified, maintainable approach.

Key Components:
    - ProtoConverter: Abstract base class for all converters
    - ValueConverter: Specialized converter for proto Value types
    - ConverterRegistry: Centralized registry for all converters
    - ProtoSerializable: Mixin for objects with proto representations

Example:
    >>> from feast.proto_conversion import get_registry, ProtoConverter
    >>>
    >>> # Get the global registry
    >>> registry = get_registry()
    >>>
    >>> # Convert an object to proto
    >>> proto = registry.to_proto(my_object)
    >>>
    >>> # Convert proto back to object
    >>> obj = registry.from_proto(proto)

Exception Hierarchy:
    - ProtoConversionError: Base exception for all conversion errors
    - UnsupportedTypeError: Unknown type encountered
    - ConverterNotFoundError: No converter registered
    - SerializationError: to_proto conversion failed
    - DeserializationError: from_proto conversion failed
    - ValidationError: Semantic validation failed
    - TypeMappingError: Type mapping failed
    - ArrowConversionError: Arrow/DataFrame conversion failed

Backward Compatibility:
    For gradual migration from the old type_map.py functions, see the
    feast.proto_conversion.compat module which provides drop-in replacements.
"""

from feast.proto_conversion.converter import ProtoConverter, ValueConverter
from feast.proto_conversion.converters import (
    DataFrameProtoConverter,
    EntityConverter,
    FeatureServiceConverter,
    FeatureViewConverter,
    OnDemandFeatureViewConverter,
    ValueTypeConverter,
    convert_arrow_to_proto,
    get_value_converter,
    proto_value_to_python,
    python_values_to_proto_values,
)
from feast.proto_conversion.errors import (
    ArrowConversionError,
    ConverterNotFoundError,
    DeserializationError,
    ProtoConversionError,
    SerializationError,
    TypeMappingError,
    UnsupportedTypeError,
    ValidationError,
)
from feast.proto_conversion.registration import (
    ensure_converters_registered,
    register_all_converters,
    reset_registration,
)
from feast.proto_conversion.registry import ConverterRegistry, get_registry
from feast.proto_conversion.serializable import (
    LegacyProtoSerializable,
    ProtoSerializable,
)

__all__ = [
    # Core interfaces
    "ProtoConverter",
    "ValueConverter",
    # Value conversion
    "ValueTypeConverter",
    "get_value_converter",
    "proto_value_to_python",
    "python_values_to_proto_values",
    # DataFrame/Arrow conversion
    "DataFrameProtoConverter",
    "convert_arrow_to_proto",
    # Object converters
    "EntityConverter",
    "FeatureViewConverter",
    "OnDemandFeatureViewConverter",
    "FeatureServiceConverter",
    # Registry
    "ConverterRegistry",
    "get_registry",
    # Registration
    "register_all_converters",
    "ensure_converters_registered",
    "reset_registration",
    # Mixins
    "ProtoSerializable",
    "LegacyProtoSerializable",
    # Exceptions
    "ProtoConversionError",
    "UnsupportedTypeError",
    "ConverterNotFoundError",
    "SerializationError",
    "DeserializationError",
    "ValidationError",
    "TypeMappingError",
    "ArrowConversionError",
]
