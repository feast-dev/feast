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
Core ProtoConverter interface and base implementations.

This module defines the abstract base class for all proto converters,
providing a unified interface for bidirectional conversion between
Python objects and their protobuf representations.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Type, TypeVar

from google.protobuf.message import Message

from feast.proto_conversion.errors import DeserializationError, SerializationError

if TYPE_CHECKING:
    from feast.protos.feast.types.Value_pb2 import Value as ProtoValue

# Type variables for generic converter interface
T = TypeVar("T")  # Python object type
P = TypeVar("P", bound=Message)  # Protobuf message type


class ProtoConverter(ABC, Generic[T, P]):
    """
    Abstract base class for bidirectional proto conversion.

    This interface provides a unified pattern for converting between
    Python objects and their protobuf representations. All converters
    should inherit from this class and implement the abstract methods.

    Type Parameters:
        T: The Python object type being converted
        P: The protobuf message type

    Example:
        >>> class EntityConverter(ProtoConverter[Entity, EntityProto]):
        ...     @property
        ...     def supported_type(self) -> type:
        ...         return Entity
        ...
        ...     @property
        ...     def proto_type(self) -> type:
        ...         return EntityProto
        ...
        ...     def to_proto(self, obj: Entity) -> EntityProto:
        ...         # conversion logic
        ...         ...
        ...
        ...     def from_proto(self, proto: EntityProto) -> Entity:
        ...         # conversion logic
        ...         ...
    """

    @property
    @abstractmethod
    def supported_type(self) -> Type[T]:
        """
        Return the Python object type this converter handles.

        Returns:
            The type class that this converter can serialize/deserialize.
        """
        ...

    @property
    @abstractmethod
    def proto_type(self) -> Type[P]:
        """
        Return the protobuf message type this converter produces/consumes.

        Returns:
            The protobuf message class that this converter works with.
        """
        ...

    @abstractmethod
    def to_proto(self, obj: T) -> P:
        """
        Convert a Python object to its protobuf representation.

        Args:
            obj: The Python object to convert.

        Returns:
            The protobuf message representation of the object.

        Raises:
            SerializationError: If conversion fails.
        """
        ...

    @abstractmethod
    def from_proto(self, proto: P) -> T:
        """
        Convert a protobuf message to its Python object representation.

        Args:
            proto: The protobuf message to convert.

        Returns:
            The Python object representation of the proto.

        Raises:
            DeserializationError: If conversion fails.
        """
        ...

    def safe_to_proto(self, obj: T) -> P:
        """
        Safely convert to proto with standardized error handling.

        This wrapper catches any exceptions during conversion and
        re-raises them as SerializationError for consistent handling.

        Args:
            obj: The Python object to convert.

        Returns:
            The protobuf message representation.

        Raises:
            SerializationError: If conversion fails for any reason.
        """
        try:
            return self.to_proto(obj)
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(obj, self.proto_type, cause=e) from e

    def safe_from_proto(self, proto: P) -> T:
        """
        Safely convert from proto with standardized error handling.

        This wrapper catches any exceptions during conversion and
        re-raises them as DeserializationError for consistent handling.

        Args:
            proto: The protobuf message to convert.

        Returns:
            The Python object representation.

        Raises:
            DeserializationError: If conversion fails for any reason.
        """
        try:
            return self.from_proto(proto)
        except DeserializationError:
            raise
        except Exception as e:
            raise DeserializationError(proto, self.supported_type, cause=e) from e

    def validate_object(self, obj: T) -> None:
        """
        Validate that an object is suitable for conversion.

        Override this method to add validation logic before conversion.
        The default implementation checks type compatibility.

        Args:
            obj: The object to validate.

        Raises:
            ValidationError: If the object is invalid.
        """
        if not isinstance(obj, self.supported_type):
            from feast.proto_conversion.errors import ValidationError

            raise ValidationError(
                f"Expected {self.supported_type.__name__}, got {type(obj).__name__}",
                value=obj,
            )

    def validate_proto(self, proto: P) -> None:
        """
        Validate that a proto message is suitable for conversion.

        Override this method to add validation logic before conversion.
        The default implementation checks type compatibility.

        Args:
            proto: The proto to validate.

        Raises:
            ValidationError: If the proto is invalid.
        """
        if not isinstance(proto, self.proto_type):
            from feast.proto_conversion.errors import ValidationError

            raise ValidationError(
                f"Expected {self.proto_type.__name__}, got {type(proto).__name__}",
                value=proto,
            )


class ValueConverter(ABC, Generic[T]):
    """
    Abstract base class for converting Python values to/from proto Value messages.

    This is a specialized converter for handling the ProtoValue type which
    uses a union (oneof) to represent different value types. Unlike
    ProtoConverter, this handles the complexity of determining which
    value type variant to use.

    Type Parameters:
        T: The Python value type being converted
    """

    @property
    @abstractmethod
    def supported_types(self) -> tuple[Type, ...]:
        """
        Return the Python types this converter handles.

        Returns:
            Tuple of type classes that this converter can handle.
        """
        ...

    @abstractmethod
    def can_convert(self, value: object) -> bool:
        """
        Check if this converter can handle the given value.

        Args:
            value: The value to check.

        Returns:
            True if this converter can handle the value, False otherwise.
        """
        ...

    @abstractmethod
    def to_proto_value(self, value: T) -> "ProtoValue":  # noqa: F821
        """
        Convert a Python value to a ProtoValue message.

        Args:
            value: The Python value to convert.

        Returns:
            The ProtoValue message representation.

        Raises:
            SerializationError: If conversion fails.
        """
        ...

    @abstractmethod
    def from_proto_value(self, proto: "ProtoValue") -> T:  # noqa: F821
        """
        Convert a ProtoValue message to a Python value.

        Args:
            proto: The ProtoValue message to convert.

        Returns:
            The Python value.

        Raises:
            DeserializationError: If conversion fails.
        """
        ...


# Import ProtoValue here to avoid circular imports
# This is done at module level for type checking but the actual
# import happens at runtime
def _get_proto_value_type():
    """Lazy import of ProtoValue to avoid circular imports."""
    from feast.protos.feast.types.Value_pb2 import Value as ProtoValue

    return ProtoValue
