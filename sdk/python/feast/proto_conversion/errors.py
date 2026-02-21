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
Exception hierarchy for protobuf conversion errors.

This module provides a unified exception hierarchy for all protobuf
conversion-related errors, replacing scattered assertions, generic
exceptions, and logging-based error handling with proper typed exceptions.
"""

from typing import Any, Optional, Type


class ProtoConversionError(Exception):
    """
    Base exception for all protobuf conversion errors.

    All conversion-related exceptions inherit from this class, enabling
    consistent error handling across the conversion system.
    """

    def __init__(self, message: str, cause: Optional[Exception] = None):
        super().__init__(message)
        self.cause = cause

    def __str__(self) -> str:
        if self.cause:
            return f"{super().__str__()} (caused by: {self.cause})"
        return super().__str__()


class UnsupportedTypeError(ProtoConversionError):
    """
    Raised when attempting to convert an unsupported type.

    This replaces the scattered TypeError and ValueError instances
    throughout the codebase when encountering unknown types.
    """

    def __init__(
        self,
        value: Any,
        expected_types: Optional[list[Type]] = None,
        context: Optional[str] = None,
    ):
        self.value = value
        self.actual_type = type(value)
        self.expected_types = expected_types or []
        self.context = context

        msg_parts = [f"Unsupported type: {self.actual_type.__name__}"]
        if self.expected_types:
            type_names = [t.__name__ for t in self.expected_types]
            msg_parts.append(f"expected one of: {', '.join(type_names)}")
        if self.context:
            msg_parts.append(f"context: {self.context}")

        super().__init__(" | ".join(msg_parts))


class ConverterNotFoundError(ProtoConversionError):
    """
    Raised when no converter is registered for a given type.

    This replaces KeyError and AttributeError instances when looking
    up converters in the registry.
    """

    def __init__(self, obj_type: Type, direction: str = "to_proto"):
        self.obj_type = obj_type
        self.direction = direction
        super().__init__(
            f"No converter registered for type '{obj_type.__name__}' "
            f"(direction: {direction})"
        )


class SerializationError(ProtoConversionError):
    """
    Raised when serialization to protobuf fails.

    This wraps errors that occur during the to_proto conversion process.
    """

    def __init__(
        self,
        obj: Any,
        target_proto_type: Optional[Type] = None,
        cause: Optional[Exception] = None,
    ):
        self.obj = obj
        self.obj_type = type(obj)
        self.target_proto_type = target_proto_type

        msg = f"Failed to serialize {self.obj_type.__name__}"
        if target_proto_type:
            msg += f" to {target_proto_type.__name__}"

        super().__init__(msg, cause)


class DeserializationError(ProtoConversionError):
    """
    Raised when deserialization from protobuf fails.

    This wraps errors that occur during the from_proto conversion process.
    """

    def __init__(
        self,
        proto: Any,
        target_type: Optional[Type] = None,
        cause: Optional[Exception] = None,
    ):
        self.proto = proto
        self.proto_type = type(proto)
        self.target_type = target_type

        msg = f"Failed to deserialize {self.proto_type.__name__}"
        if target_type:
            msg += f" to {target_type.__name__}"

        super().__init__(msg, cause)


class ValidationError(ProtoConversionError):
    """
    Raised when validation of proto data fails.

    This is used for semantic validation errors, such as missing
    required fields or invalid field combinations.
    """

    def __init__(
        self,
        message: str,
        field_name: Optional[str] = None,
        value: Any = None,
    ):
        self.field_name = field_name
        self.value = value

        if field_name:
            message = f"Validation error for field '{field_name}': {message}"

        super().__init__(message)


class TypeMappingError(ProtoConversionError):
    """
    Raised when type mapping between Python and Proto types fails.

    This consolidates errors from type_map.py related to type inference
    and conversion mapping.
    """

    def __init__(
        self,
        source_type: Type,
        target_domain: str = "proto",
        cause: Optional[Exception] = None,
    ):
        self.source_type = source_type
        self.target_domain = target_domain

        super().__init__(
            f"Cannot map type '{source_type.__name__}' to {target_domain} type",
            cause,
        )


class ArrowConversionError(ProtoConversionError):
    """
    Raised when Arrow/DataFrame to proto conversion fails.

    This consolidates errors from the _convert_arrow_* family of functions.
    """

    def __init__(
        self,
        message: str,
        column_name: Optional[str] = None,
        arrow_type: Optional[str] = None,
        cause: Optional[Exception] = None,
    ):
        self.column_name = column_name
        self.arrow_type = arrow_type

        if column_name:
            message = f"Arrow conversion error for column '{column_name}': {message}"
        if arrow_type:
            message += f" (Arrow type: {arrow_type})"

        super().__init__(message, cause)
