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
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

from google.protobuf.duration_pb2 import Duration
from google.protobuf.json_format import MessageToJson

from feast import utils
from feast.data_source import DataSource
from feast.errors import RegistryInferenceFailure
from feast.feature import Feature
from feast.feature_view_projection import FeatureViewProjection
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
from feast.repo_config import RepoConfig
from feast.usage import log_exceptions
from feast.value_type import ValueType

warnings.simplefilter("once", DeprecationWarning)

# DUMMY_ENTITY is a placeholder entity used in entityless FeatureViews
DUMMY_ENTITY_ID = "__dummy_id"
DUMMY_ENTITY_NAME = "__dummy"
DUMMY_ENTITY_VAL = ""


class FeatureView:
    """
    A FeatureView defines a logical grouping of serveable features.

    Args:
        name: Name of the group of features.
        entities: The entities to which this group of features is associated.
        ttl: The amount of time this group of features lives. A ttl of 0 indicates that
            this group of features lives forever. Note that large ttl's or a ttl of 0
            can result in extremely computationally intensive queries.
        input: The source of data where this group of features is stored.
        batch_source (optional): The batch source of data where this group of features
            is stored.
        stream_source (optional): The stream source of data where this group of features
            is stored.
        features (optional): The set of features defined as part of this FeatureView.
        tags (optional): A dictionary of key-value pairs used for organizing
            FeatureViews.
    """

    name: str
    entities: List[str]
    features: List[Feature]
    tags: Optional[Dict[str, str]]
    ttl: timedelta
    online: bool
    input: DataSource
    batch_source: DataSource
    stream_source: Optional[DataSource] = None
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None
    materialization_intervals: List[Tuple[datetime, datetime]]

    @log_exceptions
    def __init__(
        self,
        name: str,
        entities: List[str],
        ttl: Union[Duration, timedelta],
        input: Optional[DataSource] = None,
        batch_source: Optional[DataSource] = None,
        stream_source: Optional[DataSource] = None,
        features: Optional[List[Feature]] = None,
        tags: Optional[Dict[str, str]] = None,
        online: bool = True,
    ):
        """
        Creates a FeatureView object.

        Raises:
            ValueError: A field mapping conflicts with an Entity or a Feature.
        """
        if input is not None:
            warnings.warn(
                (
                    "The argument 'input' is being deprecated. Please use 'batch_source' "
                    "instead. Feast 0.13 and onwards will not support the argument 'input'."
                ),
                DeprecationWarning,
            )

        _input = input or batch_source
        assert _input is not None

        _features = features or []

        cols = [entity for entity in entities] + [feat.name for feat in _features]
        for col in cols:
            if _input.field_mapping is not None and col in _input.field_mapping.keys():
                raise ValueError(
                    f"The field {col} is mapped to {_input.field_mapping[col]} for this data source. "
                    f"Please either remove this field mapping or use {_input.field_mapping[col]} as the "
                    f"Entity or Feature name."
                )

        self.name = name
        self.entities = entities if entities else [DUMMY_ENTITY_NAME]
        self.features = _features
        self.tags = tags if tags is not None else {}

        if isinstance(ttl, Duration):
            self.ttl = timedelta(seconds=int(ttl.seconds))
        else:
            self.ttl = ttl

        self.online = online
        self.input = _input
        self.batch_source = _input
        self.stream_source = stream_source

        self.materialization_intervals = []

        self.created_timestamp: Optional[datetime] = None
        self.last_updated_timestamp: Optional[datetime] = None

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash(self.name)

    def __getitem__(self, item) -> FeatureViewProjection:
        assert isinstance(item, list)

        referenced_features = []
        for feature in self.features:
            if feature.name in item:
                referenced_features.append(feature)

        return FeatureViewProjection(self.name, referenced_features)

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
        if self.batch_source != other.batch_source:
            return False
        if self.stream_source != other.stream_source:
            return False

        return True

    def is_valid(self):
        """
        Validates the state of this feature view locally.

        Raises:
            ValueError: The feature view does not have a name or does not have entities.
        """
        if not self.name:
            raise ValueError("Feature view needs a name.")

        if not self.entities:
            raise ValueError("Feature view has no entities.")

    def to_proto(self) -> FeatureViewProto:
        """
        Converts a feature view object to its protobuf representation.

        Returns:
            A FeatureViewProto protobuf.
        """
        meta = FeatureViewMetaProto(materialization_intervals=[])
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)
        for interval in self.materialization_intervals:
            interval_proto = MaterializationIntervalProto()
            interval_proto.start_time.FromDatetime(interval[0])
            interval_proto.end_time.FromDatetime(interval[1])
            meta.materialization_intervals.append(interval_proto)

        ttl_duration = None
        if self.ttl is not None:
            ttl_duration = Duration()
            ttl_duration.FromTimedelta(self.ttl)

        batch_source_proto = self.batch_source.to_proto()
        batch_source_proto.data_source_class_type = f"{self.batch_source.__class__.__module__}.{self.batch_source.__class__.__name__}"

        stream_source_proto = None
        if self.stream_source:
            stream_source_proto = self.stream_source.to_proto()
            stream_source_proto.data_source_class_type = f"{self.stream_source.__class__.__module__}.{self.stream_source.__class__.__name__}"

        spec = FeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            features=[feature.to_proto() for feature in self.features],
            tags=self.tags,
            ttl=(ttl_duration if ttl_duration is not None else None),
            online=self.online,
            batch_source=batch_source_proto,
            stream_source=stream_source_proto,
        )

        return FeatureViewProto(spec=spec, meta=meta)

    @classmethod
    def from_proto(cls, feature_view_proto: FeatureViewProto):
        """
        Creates a feature view from a protobuf representation of a feature view.

        Args:
            feature_view_proto: A protobuf representation of a feature view.

        Returns:
            A FeatureViewProto object based on the feature view protobuf.
        """
        batch_source = DataSource.from_proto(feature_view_proto.spec.batch_source)
        stream_source = (
            DataSource.from_proto(feature_view_proto.spec.stream_source)
            if feature_view_proto.spec.HasField("stream_source")
            else None
        )
        feature_view = cls(
            name=feature_view_proto.spec.name,
            entities=[entity for entity in feature_view_proto.spec.entities],
            features=[
                Feature(
                    name=feature.name,
                    dtype=ValueType(feature.value_type),
                    labels=dict(feature.labels),
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
            batch_source=batch_source,
            stream_source=stream_source,
        )

        if feature_view_proto.meta.HasField("created_timestamp"):
            feature_view.created_timestamp = (
                feature_view_proto.meta.created_timestamp.ToDatetime()
            )
        if feature_view_proto.meta.HasField("last_updated_timestamp"):
            feature_view.last_updated_timestamp = (
                feature_view_proto.meta.last_updated_timestamp.ToDatetime()
            )

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
        """
        Retrieves the latest time up to which the feature view has been materialized.

        Returns:
            The latest time, or None if the feature view has not been materialized.
        """
        if len(self.materialization_intervals) == 0:
            return None
        return max([interval[1] for interval in self.materialization_intervals])

    def infer_features_from_batch_source(self, config: RepoConfig):
        """
        Infers the set of features associated to this feature view from the input source.

        Args:
            config: Configuration object used to configure the feature store.

        Raises:
            RegistryInferenceFailure: The set of features could not be inferred.
        """
        if not self.features:
            columns_to_exclude = {
                self.batch_source.event_timestamp_column,
                self.batch_source.created_timestamp_column,
            } | set(self.entities)

            if (
                self.batch_source.event_timestamp_column
                in self.batch_source.field_mapping
            ):
                columns_to_exclude.add(
                    self.batch_source.field_mapping[
                        self.batch_source.event_timestamp_column
                    ]
                )
            if (
                self.batch_source.created_timestamp_column
                in self.batch_source.field_mapping
            ):
                columns_to_exclude.add(
                    self.batch_source.field_mapping[
                        self.batch_source.created_timestamp_column
                    ]
                )
            for e in self.entities:
                if e in self.batch_source.field_mapping:
                    columns_to_exclude.add(self.batch_source.field_mapping[e])

            for (
                col_name,
                col_datatype,
            ) in self.batch_source.get_table_column_names_and_types(config):
                if col_name not in columns_to_exclude and not re.match(
                    "^__|__$",
                    col_name,  # double underscores often signal an internal-use column
                ):
                    feature_name = (
                        self.batch_source.field_mapping[col_name]
                        if col_name in self.batch_source.field_mapping
                        else col_name
                    )
                    self.features.append(
                        Feature(
                            feature_name,
                            self.batch_source.source_datatype_to_feast_value_type()(
                                col_datatype
                            ),
                        )
                    )

            if not self.features:
                raise RegistryInferenceFailure(
                    "FeatureView",
                    f"Could not infer Features for the FeatureView named {self.name}.",
                )
