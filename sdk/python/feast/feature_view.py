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
from feast.parquet_source import ParquetSource
from feast.value_type import ValueType


class FeatureView:
    """
    A FeatureView defines a logical grouping of serveable features.
    """

    name: str
    entities: List[str]
    features: List[Feature]
    tags: Optional[Dict[str, str]]
    ttl: Optional[timedelta]
    online: bool
    input: Union[BigQuerySource, ParquetSource]

    created_timestamp: Optional[Timestamp] = None
    last_updated_timestamp: Optional[Timestamp] = None

    def __init__(
        self,
        name: str,
        entities: List[str],
        features: List[Feature],
        ttl: Optional[Union[Duration, timedelta]],
        input: Union[BigQuerySource, ParquetSource],
        tags: Optional[Dict[str, str]] = None,
        online: bool = True,
    ):
        cols = [entity for entity in entities] + [feat.name for feat in features]
        for col in cols:
            if input.field_mapping is not None and col in input.field_mapping.keys():
                raise ValueError(
                    f"The field {col} is mapped to {input.field_mapping[col]} for this data source. Please either remove this field mapping or use {input.field_mapping[col]} as the Entity or Feature name."
                )

        self.name = name
        self.entities = entities
        self.features = features
        self.tags = tags

        if isinstance(ttl, Duration):
            self.ttl = timedelta(seconds=int(ttl.seconds))
        else:
            self.ttl = ttl

        self.online = online
        self.input = input

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
        Converts an feature view object to its protobuf representation.

        Returns:
            FeatureViewProto protobuf
        """

        meta = FeatureViewMetaProto(
            created_timestamp=self.created_timestamp,
            last_updated_timestamp=self.last_updated_timestamp,
        )

        if self.ttl is not None:
            ttl_duration = Duration()
            ttl_duration.FromTimedelta(self.ttl)

        spec = FeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            features=[feature.to_proto() for feature in self.features],
            tags=self.tags,
            ttl=(ttl_duration if ttl_duration is not None else None),
            online=self.online,
            input=self.input.to_proto(),
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
            input=DataSource.from_proto(feature_view_proto.spec.input),
        )

        feature_view.created_timestamp = feature_view_proto.meta.created_timestamp

        return feature_view
