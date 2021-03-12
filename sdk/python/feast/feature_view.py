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
from datetime import timedelta
from typing import Dict, List, Optional, Union

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.core.FeatureView_pb2 import FeatureViewMeta as FeatureViewMetaProto
from feast.core.FeatureView_pb2 import FeatureViewSpec as FeatureViewSpecProto
from feast.data_source import BigQuerySource, DataSource
from feast.feature import Feature
from feast.value_type import ValueType


class FeatureView:
    """
    A FeatureView defines a logical grouping of servable features.
    """

    name: str
    entities: List[str]
    features: List[Feature]
    tags: Dict[str, str]
    ttl: Optional[Duration]
    online: bool
    inputs: List[BigQuerySource]

    created_timestamp: Optional[Timestamp] = None
    last_updated_timestamp: Optional[Timestamp] = None

    def __init__(
        self,
        name: str,
        entities: List[str],
        features: List[Feature],
        tags: Dict[str, str],
        ttl: Optional[Union[Duration, timedelta]],
        online: bool,
        inputs: Union[BigQuerySource, List[BigQuerySource]],
    ):
        self.name = name
        self.entities = entities
        self.features = features
        self.tags = tags
        if isinstance(ttl, timedelta):
            proto_ttl = Duration()
            proto_ttl.FromTimedelta(ttl)
            self.ttl = proto_ttl
        else:
            self.ttl = ttl

        self.online = online
        self.inputs = inputs if isinstance(inputs, list) else [inputs]

    def is_valid(self):
        """
        Validates the state of a feature view locally. Raises an exception
        if feature view is invalid.
        """

        if not self.name:
            raise ValueError("Feature view needs a name")

        if not self.entities:
            raise ValueError("Feature view has no entities")

    def to_proto(self) -> FeatureViewProto:
        """
        Converts an feature view object to its protobuf representation

        Returns:
            FeatureViewProto protobuf
        """

        meta = FeatureViewMetaProto(
            created_timestamp=self.created_timestamp,
            last_updated_timestamp=self.last_updated_timestamp,
        )

        spec = FeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            features=[feature.to_proto() for feature in self.features],
            tags=self.tags,
            ttl=self.ttl,
            online=self.online,
            inputs=[inp.to_proto() for inp in self.inputs],
        )

        return FeatureViewProto(spec=spec, meta=meta)

    @classmethod
    def from_proto(cls, feature_view_proto: FeatureViewProto):
        """
        Creates a feature view from a protobuf representation of a feature view

        Args:
            feature_view_proto: A protobuf representation of a feature view

        Returns:
            Returns a FeatureViewProto object based on the feature view protobuf
        """

        feature_view = cls(
            name=feature_view_proto.spec.name,
            entities=[entity for entity in feature_view_proto.spec.entities],
            features=[
                Feature(
                    name=feature.name,
                    dtype=ValueType(feature.value_type),
                    labels=feature.labels,
                )
                for feature in feature_view_proto.spec.features
            ],
            tags=dict(feature_view_proto.spec.tags),
            online=feature_view_proto.spec.online,
            ttl=(
                None
                if feature_view_proto.spec.ttl.seconds == 0
                and feature_view_proto.spec.ttl.nanos == 0
                else feature_view_proto.spec.ttl
            ),
            inputs=[
                DataSource.from_proto(inp) for inp in feature_view_proto.spec.inputs
            ],
        )

        feature_view.created_timestamp = feature_view_proto.meta.created_timestamp

        return feature_view
