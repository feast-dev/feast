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
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

from google.protobuf.duration_pb2 import Duration
from google.protobuf.json_format import MessageToJson
from google.protobuf.timestamp_pb2 import Timestamp

from feast import utils
from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.feature import Feature
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.FeatureView_pb2 import (
    FeatureViewMeta as FeatureViewMetaProto,
)
from feast.protos.feast.core.FeatureView_pb2 import (
    FeatureViewSpec as FeatureViewSpecProto,
)
from feast.protos.feast.core.FeatureView_pb2 import (
    MaterializationInterval as MaterializationIntervalProto,
)
from feast.telemetry import log_exceptions
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
    input: Union[BigQuerySource, FileSource]

    created_timestamp: Optional[Timestamp] = None
    last_updated_timestamp: Optional[Timestamp] = None
    materialization_intervals: List[Tuple[datetime, datetime]]

    @log_exceptions
    def __init__(
        self,
        name: str,
        entities: List[str],
        ttl: Optional[Union[Duration, timedelta]],
        input: Union[BigQuerySource, FileSource],
        features: List[Feature] = [],
        tags: Optional[Dict[str, str]] = None,
        online: bool = True,
    ):
        if not features:
            features = []  # to handle python's mutable default arguments
            columns_to_exclude = {
                input.event_timestamp_column,
                input.created_timestamp_column,
            } | set(entities)

            for col_name, col_datatype in input.get_table_column_names_and_types():
                if col_name not in columns_to_exclude and not re.match(
                    "^__|__$", col_name
                ):
                    features.append(
                        Feature(
                            col_name,
                            input.source_datatype_to_feast_value_type()(col_datatype),
                        )
                    )

            if not features:
                raise ValueError(
                    f"Could not infer Features for the FeatureView named {name}. Please specify Features explicitly for this FeatureView."
                )

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

        self.materialization_intervals = []

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, FeatureView):
            raise TypeError(
                "Comparisons should only involve FeatureView class objects."
            )

        if (
            self.tags != other.tags
            or self.name != other.name
            or self.ttl != other.ttl
            or self.online != other.online
        ):
            return False

        if sorted(self.entities) != sorted(other.entities):
            return False
        if sorted(self.features) != sorted(other.features):
            return False
        if self.input != other.input:
            return False

        return True

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
            materialization_intervals=[],
        )
        for interval in self.materialization_intervals:
            interval_proto = MaterializationIntervalProto()
            interval_proto.start_time.FromDatetime(interval[0])
            interval_proto.end_time.FromDatetime(interval[1])
            meta.materialization_intervals.append(interval_proto)

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

        for interval in feature_view_proto.meta.materialization_intervals:
            feature_view.materialization_intervals.append(
                (
                    utils.make_tzaware(interval.start_time.ToDatetime()),
                    utils.make_tzaware(interval.end_time.ToDatetime()),
                )
            )

        return feature_view

    @property
    def most_recent_end_time(self) -> Optional[datetime]:
        if len(self.materialization_intervals) == 0:
            return None
        return max([interval[1] for interval in self.materialization_intervals])
