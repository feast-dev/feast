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
FeatureServiceConverter for bidirectional FeatureService <-> FeatureServiceProto conversion.

This module provides a clean, type-safe converter for FeatureService objects.
"""

from typing import Optional, Type

from feast.feature_logging import LoggingConfig
from feast.feature_service import FeatureService
from feast.feature_view_projection import FeatureViewProjection
from feast.proto_conversion.converter import ProtoConverter
from feast.proto_conversion.errors import DeserializationError, SerializationError
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureServiceMeta as FeatureServiceMetaProto,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureServiceSpec as FeatureServiceSpecProto,
)


class FeatureServiceConverter(ProtoConverter[FeatureService, FeatureServiceProto]):
    """
    Converter for FeatureService <-> FeatureServiceProto conversion.

    This converter handles bidirectional conversion between FeatureService
    Python objects and their protobuf representation.

    Example:
        >>> converter = FeatureServiceConverter()
        >>> proto = converter.to_proto(feature_service)
        >>> restored = converter.from_proto(proto)
    """

    @property
    def supported_type(self) -> Type[FeatureService]:
        return FeatureService

    @property
    def proto_type(self) -> Type[FeatureServiceProto]:
        return FeatureServiceProto

    def to_proto(self, obj: FeatureService) -> FeatureServiceProto:
        """
        Convert a FeatureService to its protobuf representation.

        Args:
            obj: The FeatureService object to convert.

        Returns:
            The FeatureServiceProto representation.

        Raises:
            SerializationError: If conversion fails.
        """
        try:
            meta = FeatureServiceMetaProto()
            if obj.created_timestamp:
                meta.created_timestamp.FromDatetime(obj.created_timestamp)
            if obj.last_updated_timestamp:
                meta.last_updated_timestamp.FromDatetime(obj.last_updated_timestamp)

            spec = FeatureServiceSpecProto(
                name=obj.name,
                features=[
                    projection.to_proto() for projection in obj.feature_view_projections
                ],
                tags=obj.tags,
                description=obj.description,
                owner=obj.owner,
                logging_config=obj.logging_config.to_proto()
                if obj.logging_config
                else None,
            )

            return FeatureServiceProto(spec=spec, meta=meta)

        except Exception as e:
            raise SerializationError(obj, FeatureServiceProto, cause=e) from e

    def from_proto(self, proto: FeatureServiceProto) -> FeatureService:
        """
        Convert a FeatureServiceProto to a FeatureService object.

        Args:
            proto: The FeatureServiceProto to convert.

        Returns:
            The FeatureService object.

        Raises:
            DeserializationError: If conversion fails.
        """
        try:
            logging_config: Optional[LoggingConfig] = None
            if proto.spec.HasField("logging_config"):
                logging_config = LoggingConfig.from_proto(proto.spec.logging_config)

            fs = FeatureService(
                name=proto.spec.name,
                features=[],
                tags=dict(proto.spec.tags),
                description=proto.spec.description,
                owner=proto.spec.owner,
                logging_config=logging_config,
            )

            fs.feature_view_projections.extend(
                [
                    FeatureViewProjection.from_proto(projection)
                    for projection in proto.spec.features
                ]
            )

            if proto.meta.HasField("created_timestamp"):
                fs.created_timestamp = proto.meta.created_timestamp.ToDatetime()
            if proto.meta.HasField("last_updated_timestamp"):
                fs.last_updated_timestamp = (
                    proto.meta.last_updated_timestamp.ToDatetime()
                )

            return fs

        except Exception as e:
            raise DeserializationError(proto, FeatureService, cause=e) from e
