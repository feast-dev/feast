# Copyright 2019 The Feast Authors
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
Utility functions for protobuf serialization of Feast objects.
"""

from typing import TYPE_CHECKING, Any, Optional, Union

from google.protobuf.message import Message

from feast.protos.feast.core.Transformation_pb2 import (
    FeatureTransformationV2 as FeatureTransformationProto,
)
from feast.protos.feast.core.Transformation_pb2 import (
    SubstraitTransformationV2 as SubstraitTransformationProto,
)
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)

if TYPE_CHECKING:
    from feast.data_source import DataSource
    from feast.transformation.mode import TransformationMode


def serialize_data_source(source: Optional["DataSource"]) -> Optional[Message]:
    """Serialize a data source to proto with class type annotation.

    Args:
        source: The data source to serialize, or None.

    Returns:
        The serialized proto with data_source_class_type set, or None if source is None.
    """
    if source is None:
        return None
    proto = source.to_proto()
    proto.data_source_class_type = (
        f"{source.__class__.__module__}.{source.__class__.__name__}"
    )
    return proto


def transformation_to_proto(
    transformation: Optional[Any],
) -> Optional[FeatureTransformationProto]:
    """Convert a transformation to FeatureTransformationProto.

    Args:
        transformation: The transformation object with a to_proto() method.

    Returns:
        A FeatureTransformationProto wrapping the transformation, or None.
    """
    if transformation is None:
        return None

    if not hasattr(transformation, "to_proto"):
        return None

    transformation_proto = transformation.to_proto()

    if isinstance(transformation_proto, UserDefinedFunctionProto):
        return FeatureTransformationProto(
            user_defined_function=transformation_proto,
        )
    elif isinstance(transformation_proto, SubstraitTransformationProto):
        return FeatureTransformationProto(
            substrait_transformation=transformation_proto,
        )
    return None


def mode_to_string(mode: Optional[Union["TransformationMode", str]]) -> str:
    """Convert mode to string value.

    Args:
        mode: A TransformationMode enum or string, or None.

    Returns:
        The string representation of the mode, or empty string if None.
    """
    from feast.transformation.mode import TransformationMode

    if mode is None:
        return ""
    if isinstance(mode, TransformationMode):
        return mode.value
    return mode
