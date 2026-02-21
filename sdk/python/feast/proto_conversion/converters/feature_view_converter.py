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
FeatureViewConverter for bidirectional FeatureView <-> FeatureViewProto conversion.

This module provides a converter for FeatureView objects. Due to the complexity
of FeatureView (nested structures, cycle detection, multiple subclasses),
this converter delegates to the existing to_proto/from_proto methods while
providing the standardized ProtoConverter interface.

The FeatureView hierarchy includes:
- FeatureView (base class for batch feature views)
- BatchFeatureView (with transformations)
- StreamFeatureView (with stream source)
- OnDemandFeatureView (computed on-demand)
"""

from typing import Any, Dict, Optional, Type, Union, cast

from feast.proto_conversion.converter import ProtoConverter
from feast.proto_conversion.errors import DeserializationError, SerializationError
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto


class FeatureViewConverter(ProtoConverter[Any, FeatureViewProto]):
    """
    Converter for FeatureView <-> FeatureViewProto conversion.

    This converter handles the FeatureView class and its subclasses
    (BatchFeatureView, StreamFeatureView). Due to the complexity of the
    FeatureView hierarchy with nested source views and cycle detection,
    this converter delegates to the existing to_proto/from_proto methods.

    The converter provides:
    - Standardized ProtoConverter interface
    - Cycle detection for nested structures
    - Support for FeatureView subclasses

    Note:
        OnDemandFeatureView has a separate converter (OnDemandFeatureViewConverter)
        due to its different proto type and conversion logic.

    Example:
        >>> from feast.feature_view import FeatureView
        >>> converter = FeatureViewConverter()
        >>> proto = converter.to_proto(feature_view)
        >>> restored = converter.from_proto(proto)
    """

    def __init__(self):
        """Initialize the converter."""
        self._feature_view_class: Optional[Type[Any]] = None

    @property
    def supported_type(self) -> Type[Any]:
        """Return the FeatureView class."""
        if self._feature_view_class is None:
            from feast.feature_view import FeatureView

            self._feature_view_class = FeatureView
        return cast(Type[Any], self._feature_view_class)

    @property
    def proto_type(self) -> Type[FeatureViewProto]:
        return FeatureViewProto

    def to_proto(
        self,
        obj: Any,
        seen: Optional[Dict[str, Union[None, FeatureViewProto]]] = None,
    ) -> FeatureViewProto:
        """
        Convert a FeatureView to its protobuf representation.

        Args:
            obj: The FeatureView object to convert.
            seen: Optional dict for cycle detection during recursive conversion.

        Returns:
            The FeatureViewProto representation.

        Raises:
            SerializationError: If conversion fails.
            ValueError: If a cycle is detected in nested FeatureViews.
        """
        try:
            if seen is not None:
                # Use the internal method with cycle detection
                return obj._to_proto_internal(seen)
            else:
                # Use the public method which initializes its own seen dict
                return obj.to_proto()

        except ValueError:
            # Re-raise cycle detection errors as-is
            raise
        except Exception as e:
            raise SerializationError(obj, FeatureViewProto, cause=e) from e

    def from_proto(
        self,
        proto: FeatureViewProto,
        seen: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Convert a FeatureViewProto to a FeatureView object.

        Args:
            proto: The FeatureViewProto to convert.
            seen: Optional dict for cycle detection during recursive conversion.

        Returns:
            The FeatureView object (may be BatchFeatureView or StreamFeatureView
            depending on the proto contents).

        Raises:
            DeserializationError: If conversion fails.
            ValueError: If a cycle is detected in nested FeatureViews.
        """
        try:
            from feast.feature_view import FeatureView

            if seen is not None:
                return FeatureView._from_proto_internal(proto, seen)
            else:
                return FeatureView.from_proto(proto)

        except ValueError:
            # Re-raise cycle detection errors as-is
            raise
        except Exception as e:
            raise DeserializationError(proto, self.supported_type, cause=e) from e

    def validate_object(self, obj: Any) -> None:
        """
        Validate that an object is a FeatureView.

        Accepts FeatureView and its subclasses (BatchFeatureView, StreamFeatureView).
        """
        from feast.feature_view import FeatureView

        if not isinstance(obj, FeatureView):
            from feast.proto_conversion.errors import ValidationError

            raise ValidationError(
                f"Expected FeatureView or subclass, got {type(obj).__name__}",
                value=obj,
            )


class OnDemandFeatureViewConverter(ProtoConverter[Any, Any]):
    """
    Converter for OnDemandFeatureView <-> OnDemandFeatureViewProto conversion.

    OnDemandFeatureView has a separate proto type and different conversion
    logic than FeatureView, so it requires its own converter.

    Example:
        >>> from feast.on_demand_feature_view import OnDemandFeatureView
        >>> converter = OnDemandFeatureViewConverter()
        >>> proto = converter.to_proto(odfv)
        >>> restored = converter.from_proto(proto)
    """

    def __init__(self):
        """Initialize the converter."""
        self._odfv_class: Optional[Type[Any]] = None
        self._proto_class: Optional[Type[Any]] = None

    @property
    def supported_type(self) -> Type[Any]:
        """Return the OnDemandFeatureView class."""
        if self._odfv_class is None:
            from feast.on_demand_feature_view import OnDemandFeatureView

            self._odfv_class = OnDemandFeatureView
        return cast(Type[Any], self._odfv_class)

    @property
    def proto_type(self) -> Type[Any]:
        """Return the OnDemandFeatureViewProto class."""
        if self._proto_class is None:
            from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
                OnDemandFeatureView as OnDemandFeatureViewProto,
            )

            self._proto_class = OnDemandFeatureViewProto
        return cast(Type[Any], self._proto_class)

    def to_proto(self, obj: Any) -> Any:
        """
        Convert an OnDemandFeatureView to its protobuf representation.

        Args:
            obj: The OnDemandFeatureView object to convert.

        Returns:
            The OnDemandFeatureViewProto representation.

        Raises:
            SerializationError: If conversion fails.
        """
        try:
            return obj.to_proto()
        except Exception as e:
            raise SerializationError(obj, self.proto_type, cause=e) from e

    def from_proto(self, proto: Any) -> Any:
        """
        Convert an OnDemandFeatureViewProto to an OnDemandFeatureView object.

        Args:
            proto: The OnDemandFeatureViewProto to convert.

        Returns:
            The OnDemandFeatureView object.

        Raises:
            DeserializationError: If conversion fails.
        """
        try:
            from feast.on_demand_feature_view import OnDemandFeatureView

            return OnDemandFeatureView.from_proto(proto)
        except Exception as e:
            raise DeserializationError(proto, self.supported_type, cause=e) from e

    def validate_object(self, obj: Any) -> None:
        """Validate that an object is an OnDemandFeatureView."""
        from feast.on_demand_feature_view import OnDemandFeatureView

        if not isinstance(obj, OnDemandFeatureView):
            from feast.proto_conversion.errors import ValidationError

            raise ValidationError(
                f"Expected OnDemandFeatureView, got {type(obj).__name__}",
                value=obj,
            )
