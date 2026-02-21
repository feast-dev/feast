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
Concrete converter implementations for Feast objects.

This subpackage contains all the specific converter implementations
for different Feast object types (Entity, FeatureView, etc.) and
value types.
"""

from feast.proto_conversion.converters.dataframe_converter import (
    DataFrameProtoConverter,
    convert_arrow_to_proto,
)
from feast.proto_conversion.converters.entity_converter import EntityConverter
from feast.proto_conversion.converters.feature_service_converter import (
    FeatureServiceConverter,
)
from feast.proto_conversion.converters.feature_view_converter import (
    FeatureViewConverter,
    OnDemandFeatureViewConverter,
)
from feast.proto_conversion.converters.value_converter import (
    ValueTypeConverter,
    get_value_converter,
    proto_value_to_python,
    python_values_to_proto_values,
)

__all__ = [
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
]
