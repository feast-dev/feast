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
Centralized ConverterRegistry for protobuf conversions.

This module provides a single registry for all proto conversion logic,
enabling type-safe lookup and conversion methods. The registry supports
both object converters (ProtoConverter) and value converters (ValueConverter).
"""

import logging
from threading import Lock
from typing import Any, Dict, List, Optional, Type, TypeVar

from google.protobuf.message import Message

from feast.proto_conversion.converter import ProtoConverter, ValueConverter
from feast.proto_conversion.errors import ConverterNotFoundError, UnsupportedTypeError

logger = logging.getLogger(__name__)

T = TypeVar("T")
P = TypeVar("P", bound=Message)


class ConverterRegistry:
    """
    Centralized registry for proto converters.

    This registry serves as the single source of truth for all conversion
    logic, replacing scattered conversion code throughout the codebase.
    It supports registration and lookup of both ProtoConverter and
    ValueConverter instances.

    Thread Safety:
        This registry is thread-safe for concurrent registrations and lookups.

    Example:
        >>> registry = ConverterRegistry()
        >>> registry.register(EntityConverter())
        >>> entity = registry.from_proto(entity_proto)
        >>> proto = registry.to_proto(entity)
    """

    _instance: Optional["ConverterRegistry"] = None
    _instance_lock = Lock()

    def __init__(self):
        """Initialize an empty registry."""
        self._converters: Dict[Type, ProtoConverter] = {}
        self._proto_to_converter: Dict[Type, ProtoConverter] = {}
        self._value_converters: List[ValueConverter] = []
        self._lock = Lock()

    @classmethod
    def get_instance(cls) -> "ConverterRegistry":
        """
        Get the global registry instance (singleton).

        Returns:
            The global ConverterRegistry instance.
        """
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """
        Reset the global registry instance.

        This is primarily useful for testing to ensure a clean state.
        """
        with cls._instance_lock:
            cls._instance = None

    def register(self, converter: ProtoConverter) -> None:
        """
        Register a ProtoConverter for its supported type.

        Args:
            converter: The converter to register.

        Raises:
            ValueError: If a converter is already registered for the type.
        """
        with self._lock:
            obj_type = converter.supported_type
            proto_type = converter.proto_type

            if obj_type in self._converters:
                existing = self._converters[obj_type]
                logger.warning(
                    f"Overwriting existing converter for {obj_type.__name__}: "
                    f"{type(existing).__name__} -> {type(converter).__name__}"
                )

            self._converters[obj_type] = converter
            self._proto_to_converter[proto_type] = converter

            logger.debug(
                f"Registered converter {type(converter).__name__} "
                f"for {obj_type.__name__} <-> {proto_type.__name__}"
            )

    def register_value_converter(self, converter: ValueConverter) -> None:
        """
        Register a ValueConverter for value type conversions.

        Value converters are checked in registration order when converting
        values. Earlier registered converters have higher priority.

        Args:
            converter: The value converter to register.
        """
        with self._lock:
            self._value_converters.append(converter)
            logger.debug(
                f"Registered value converter {type(converter).__name__} "
                f"for types: {converter.supported_types}"
            )

    def get_converter(self, obj_type: Type[T]) -> ProtoConverter[T, Any]:
        """
        Get the converter for a specific Python type.

        Args:
            obj_type: The Python type to get a converter for.

        Returns:
            The registered converter for the type.

        Raises:
            ConverterNotFoundError: If no converter is registered for the type.
        """
        with self._lock:
            # Direct lookup
            if obj_type in self._converters:
                return self._converters[obj_type]

            # Check for subclass match
            for registered_type, converter in self._converters.items():
                if issubclass(obj_type, registered_type):
                    return converter

            raise ConverterNotFoundError(obj_type, "to_proto")

    def get_converter_for_proto(self, proto_type: Type[P]) -> ProtoConverter[Any, P]:
        """
        Get the converter for a specific proto type.

        Args:
            proto_type: The proto message type to get a converter for.

        Returns:
            The registered converter for the proto type.

        Raises:
            ConverterNotFoundError: If no converter is registered for the type.
        """
        with self._lock:
            if proto_type in self._proto_to_converter:
                return self._proto_to_converter[proto_type]

            raise ConverterNotFoundError(proto_type, "from_proto")

    def get_value_converter(self, value: Any) -> ValueConverter:
        """
        Get a value converter that can handle the given value.

        Value converters are checked in registration order.

        Args:
            value: The value to find a converter for.

        Returns:
            A converter that can handle the value.

        Raises:
            UnsupportedTypeError: If no converter can handle the value.
        """
        with self._lock:
            for converter in self._value_converters:
                if converter.can_convert(value):
                    return converter

            raise UnsupportedTypeError(
                value,
                context="value conversion",
            )

    def has_converter(self, obj_type: Type) -> bool:
        """
        Check if a converter is registered for a type.

        Args:
            obj_type: The type to check.

        Returns:
            True if a converter is registered, False otherwise.
        """
        with self._lock:
            if obj_type in self._converters:
                return True

            # Check for subclass match
            for registered_type in self._converters:
                if issubclass(obj_type, registered_type):
                    return True

            return False

    def has_proto_converter(self, proto_type: Type) -> bool:
        """
        Check if a converter is registered for a proto type.

        Args:
            proto_type: The proto type to check.

        Returns:
            True if a converter is registered, False otherwise.
        """
        with self._lock:
            return proto_type in self._proto_to_converter

    def to_proto(self, obj: T) -> Message:
        """
        Convert a Python object to its proto representation.

        This is a convenience method that looks up the appropriate
        converter and performs the conversion.

        Args:
            obj: The object to convert.

        Returns:
            The proto representation of the object.

        Raises:
            ConverterNotFoundError: If no converter is registered.
            SerializationError: If conversion fails.
        """
        converter = self.get_converter(type(obj))
        return converter.safe_to_proto(obj)

    def from_proto(self, proto: P, target_type: Optional[Type[T]] = None) -> T:
        """
        Convert a proto message to its Python object representation.

        If target_type is not provided, the converter is looked up
        based on the proto message type.

        Args:
            proto: The proto message to convert.
            target_type: Optional target Python type (used for lookup).

        Returns:
            The Python object representation.

        Raises:
            ConverterNotFoundError: If no converter is registered.
            DeserializationError: If conversion fails.
        """
        if target_type is not None:
            converter = self.get_converter(target_type)
        else:
            converter = self.get_converter_for_proto(type(proto))

        return converter.safe_from_proto(proto)

    def registered_types(self) -> List[Type]:
        """
        Get a list of all registered Python types.

        Returns:
            List of types that have registered converters.
        """
        with self._lock:
            return list(self._converters.keys())

    def registered_proto_types(self) -> List[Type]:
        """
        Get a list of all registered proto types.

        Returns:
            List of proto types that have registered converters.
        """
        with self._lock:
            return list(self._proto_to_converter.keys())

    def clear(self) -> None:
        """
        Clear all registered converters.

        This is primarily useful for testing.
        """
        with self._lock:
            self._converters.clear()
            self._proto_to_converter.clear()
            self._value_converters.clear()


# Global registry instance accessor
def get_registry() -> ConverterRegistry:
    """
    Get the global converter registry instance.

    Returns:
        The global ConverterRegistry singleton.
    """
    return ConverterRegistry.get_instance()
