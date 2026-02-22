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
ProtoConverter base class for bidirectional proto conversion.

This module defines a simple abstract base class for converters that
handle conversion between Python objects and their protobuf representations.
"""

from abc import ABC, abstractmethod
from typing import Generic, Type, TypeVar

from google.protobuf.message import Message

from feast.proto_conversion.errors import DeserializationError, SerializationError

# Type variables for generic converter interface
T = TypeVar("T")  # Python object type
P = TypeVar("P", bound=Message)  # Protobuf message type


class ProtoConverter(ABC, Generic[T, P]):
    """
    Abstract base class for bidirectional proto conversion.

    Provides a simple interface for converting between Python objects
    and their protobuf representations.

    Type Parameters:
        T: The Python object type being converted
        P: The protobuf message type
    """

    @property
    @abstractmethod
    def supported_type(self) -> Type[T]:
        """Return the Python object type this converter handles."""
        ...

    @property
    @abstractmethod
    def proto_type(self) -> Type[P]:
        """Return the protobuf message type this converter produces/consumes."""
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
        """Convert to proto with standardized error handling."""
        try:
            return self.to_proto(obj)
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(obj, self.proto_type, cause=e) from e

    def safe_from_proto(self, proto: P) -> T:
        """Convert from proto with standardized error handling."""
        try:
            return self.from_proto(proto)
        except DeserializationError:
            raise
        except Exception as e:
            raise DeserializationError(proto, self.supported_type, cause=e) from e
