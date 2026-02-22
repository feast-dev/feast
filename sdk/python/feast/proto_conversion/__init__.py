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
Proto conversion utilities for Feast.

This module provides converters that consolidate duplicated proto conversion
logic into reusable classes:

- DataFrameProtoConverter: Unifies Arrow/DataFrame to proto conversion
- ValueTypeConverter: Consolidates value type conversion logic
- EntityConverter: Entity <-> EntityProto conversion
- FeatureViewConverter: FeatureView <-> FeatureViewProto conversion
- FeatureServiceConverter: FeatureService <-> FeatureServiceProto conversion
"""

from feast.proto_conversion.converter import ProtoConverter
from feast.proto_conversion.converters import (
    DataFrameProtoConverter,
    EntityConverter,
    FeatureServiceConverter,
    FeatureViewConverter,
    OnDemandFeatureViewConverter,
    ValueTypeConverter,
    convert_arrow_to_proto,
    python_values_to_proto_values,
)
from feast.proto_conversion.errors import (
    ArrowConversionError,
    DeserializationError,
    ProtoConversionError,
    SerializationError,
)

__all__ = [
    # Base class
    "ProtoConverter",
    # Converters
    "ValueTypeConverter",
    "DataFrameProtoConverter",
    "EntityConverter",
    "FeatureViewConverter",
    "OnDemandFeatureViewConverter",
    "FeatureServiceConverter",
    # Convenience functions
    "convert_arrow_to_proto",
    "python_values_to_proto_values",
    # Errors
    "ProtoConversionError",
    "SerializationError",
    "DeserializationError",
    "ArrowConversionError",
]
