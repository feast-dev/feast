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
ProtoSerializable mixin for standardized proto conversion.

This module provides a mixin class that can be added to any Feast object
to provide standardized to_proto()/from_proto() methods that delegate
to the converter registry.
"""

from typing import ClassVar, Type, TypeVar

from google.protobuf.message import Message

T = TypeVar("T", bound="ProtoSerializable")
P = TypeVar("P", bound=Message)


class ProtoSerializable:
    """
    Mixin class providing standardized proto serialization.

    Classes that inherit from this mixin can use the to_proto() and
    from_proto() methods, which delegate to the converter registry.
    This eliminates boilerplate while maintaining a consistent interface.

    The mixin requires that a converter be registered for the class
    in the global converter registry.

    Class Attributes:
        _proto_class: Optional proto class hint for deserialization.
                      If not set, the registry uses the converter's
                      declared proto_type.

    Example:
        >>> class MyFeatureView(ProtoSerializable):
        ...     _proto_class = FeatureViewProto
        ...     ...
        ...
        >>> # After registering a converter:
        >>> fv = MyFeatureView(...)
        >>> proto = fv.to_proto()
        >>> restored = MyFeatureView.from_proto(proto)
    """

    _proto_class: ClassVar[Type[Message]] = None  # type: ignore[assignment]

    def to_proto(self: T) -> Message:
        """
        Convert this object to its protobuf representation.

        Returns:
            The protobuf message representation of this object.

        Raises:
            ConverterNotFoundError: If no converter is registered for this type.
            SerializationError: If conversion fails.
        """
        from feast.proto_conversion.registry import get_registry

        registry = get_registry()
        return registry.to_proto(self)

    @classmethod
    def from_proto(cls: Type[T], proto: Message) -> T:
        """
        Create an instance from a protobuf message.

        Args:
            proto: The protobuf message to convert.

        Returns:
            An instance of this class.

        Raises:
            ConverterNotFoundError: If no converter is registered for this type.
            DeserializationError: If conversion fails.
        """
        from feast.proto_conversion.registry import get_registry

        registry = get_registry()
        return registry.from_proto(proto, target_type=cls)


class LegacyProtoSerializable:
    """
    Mixin for classes that need to maintain backward compatibility.

    This mixin allows classes to use both the legacy to_proto()/from_proto()
    methods and the new registry-based conversion. During the migration
    period, classes can inherit from this to provide both interfaces.

    The _use_legacy_conversion flag controls which path is used.
    Once migration is complete, classes should switch to ProtoSerializable.
    """

    _use_legacy_conversion: ClassVar[bool] = True

    def to_proto(self) -> Message:
        """
        Convert to proto using legacy or registry-based method.

        During migration, this delegates to _to_proto_legacy() if
        _use_legacy_conversion is True, otherwise uses the registry.
        """
        if self._use_legacy_conversion:
            return self._to_proto_legacy()

        from feast.proto_conversion.registry import get_registry

        return get_registry().to_proto(self)

    def _to_proto_legacy(self) -> Message:
        """
        Legacy to_proto implementation.

        Subclasses should override this with their existing to_proto logic
        during the migration period.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement _to_proto_legacy()"
        )

    @classmethod
    def from_proto(cls: Type[T], proto: Message) -> T:
        """
        Create from proto using legacy or registry-based method.

        During migration, this delegates to _from_proto_legacy() if
        _use_legacy_conversion is True, otherwise uses the registry.
        """
        if cls._use_legacy_conversion:
            return cls._from_proto_legacy(proto)

        from feast.proto_conversion.registry import get_registry

        return get_registry().from_proto(proto, target_type=cls)

    @classmethod
    def _from_proto_legacy(cls: Type[T], proto: Message) -> T:
        """
        Legacy from_proto implementation.

        Subclasses should override this with their existing from_proto logic
        during the migration period.
        """
        raise NotImplementedError(f"{cls.__name__} must implement _from_proto_legacy()")
