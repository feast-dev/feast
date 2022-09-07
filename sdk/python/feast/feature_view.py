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
import copy
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Type

from google.protobuf.duration_pb2 import Duration
from typeguard import typechecked

from feast import utils
from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource, KafkaSource, KinesisSource, PushSource
from feast.entity import Entity
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
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
from feast.types import from_value_type
from feast.usage import log_exceptions
from feast.value_type import ValueType

warnings.simplefilter("once", DeprecationWarning)

# DUMMY_ENTITY is a placeholder entity used in entityless FeatureViews
DUMMY_ENTITY_ID = "__dummy_id"
DUMMY_ENTITY_NAME = "__dummy"
DUMMY_ENTITY_VAL = ""
DUMMY_ENTITY = Entity(
    name=DUMMY_ENTITY_NAME,
    join_keys=[DUMMY_ENTITY_ID],
)


@typechecked
class FeatureView(BaseFeatureView):
    """
    A FeatureView defines a logical group of features.

    Attributes:
        name: The unique name of the feature view.
        entities: The list of names of entities that this feature view is associated with.
        ttl: The amount of time this group of features lives. A ttl of 0 indicates that
            this group of features lives forever. Note that large ttl's or a ttl of 0
            can result in extremely computationally intensive queries.
        batch_source: The batch source of data where this group of features
            is stored. This is optional ONLY if a push source is specified as the
            stream_source, since push sources contain their own batch sources.
        stream_source: The stream source of data where this group of features is stored.
        schema: The schema of the feature view, including feature, timestamp, and entity
            columns. If not specified, can be inferred from the underlying data source.
        entity_columns: The list of entity columns contained in the schema. If not specified,
            can be inferred from the underlying data source.
        features: The list of feature columns contained in the schema. If not specified,
            can be inferred from the underlying data source.
        online: A boolean indicating whether online retrieval is enabled for this feature
            view.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the feature view, typically the email of the primary
            maintainer.
    """

    name: str
    entities: List[str]
    ttl: Optional[timedelta]
    batch_source: DataSource
    stream_source: Optional[DataSource]
    schema: List[Field]
    entity_columns: List[Field]
    features: List[Field]
    online: bool
    description: str
    tags: Dict[str, str]
    owner: str
    materialization_intervals: List[Tuple[datetime, datetime]]

    @log_exceptions
    def __init__(
        self,
        *,
        name: str,
        source: DataSource,
        schema: Optional[List[Field]] = None,
        entities: List[Entity] = None,
        ttl: Optional[timedelta] = timedelta(days=0),
        online: bool = True,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
    ):
        """
        Creates a FeatureView object.

        Args:
            name: The unique name of the feature view.
            source: The source of data for this group of features. May be a stream source, or a batch source.
                If a stream source, the source should contain a batch_source for backfills & batch materialization.
            schema (optional): The schema of the feature view, including feature, timestamp,
                and entity columns.
            # TODO: clarify that schema is only useful here...
            entities (optional): The list of entities with which this group of features is associated.
            ttl (optional): The amount of time this group of features lives. A ttl of 0 indicates that
                this group of features lives forever. Note that large ttl's or a ttl of 0
                can result in extremely computationally intensive queries.
            online (optional): A boolean indicating whether online retrieval is enabled for
                this feature view.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the feature view, typically the email of the
                primary maintainer.

        Raises:
            ValueError: A field mapping conflicts with an Entity or a Feature.
        """
        self.name = name
        self.entities = [e.name for e in entities] if entities else [DUMMY_ENTITY_NAME]
        self.ttl = ttl
        self.schema = schema or []

        # Initialize data sources.
        if (
            isinstance(source, PushSource)
            or isinstance(source, KafkaSource)
            or isinstance(source, KinesisSource)
        ):
            self.stream_source = source
            if not source.batch_source:
                raise ValueError(
                    f"A batch_source needs to be specified for stream source `{source.name}`"
                )
            else:
                self.batch_source = source.batch_source
        else:
            self.stream_source = None
            self.batch_source = source

        # Initialize features and entity columns.
        features: List[Field] = []
        self.entity_columns = []

        join_keys: List[str] = []
        if entities:
            for entity in entities:
                join_keys.append(entity.join_key)

        # Ensure that entities have unique join keys.
        if len(set(join_keys)) < len(join_keys):
            raise ValueError(
                "A feature view should not have entities that share a join key."
            )

        for field in self.schema:
            if field.name in join_keys:
                self.entity_columns.append(field)

                # Confirm that the inferred type matches the specified entity type, if it exists.
                matching_entities = (
                    [e for e in entities if e.join_key == field.name]
                    if entities
                    else []
                )
                assert len(matching_entities) == 1
                entity = matching_entities[0]
                if entity.value_type != ValueType.UNKNOWN:
                    if from_value_type(entity.value_type) != field.dtype:
                        raise ValueError(
                            f"Entity {entity.name} has type {entity.value_type}, which does not match the inferred type {field.dtype}."
                        )
            else:
                features.append(field)

        # TODO(felixwang9817): Add more robust validation of features.
        cols = [field.name for field in self.schema]
        for col in cols:
            if (
                self.batch_source.field_mapping is not None
                and col in self.batch_source.field_mapping.keys()
            ):
                raise ValueError(
                    f"The field {col} is mapped to {self.batch_source.field_mapping[col]} for this data source. "
                    f"Please either remove this field mapping or use {self.batch_source.field_mapping[col]} as the "
                    f"Entity or Feature name."
                )

        super().__init__(
            name=name,
            features=features,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.online = online
        self.materialization_intervals = []

    def __hash__(self):
        return super().__hash__()

    def __copy__(self):
        fv = FeatureView(
            name=self.name,
            ttl=self.ttl,
            source=self.stream_source if self.stream_source else self.batch_source,
            schema=self.schema,
            tags=self.tags,
            online=self.online,
        )

        # This is deliberately set outside of the FV initialization as we do not have the Entity objects.
        fv.entities = self.entities
        fv.features = copy.copy(self.features)
        fv.entity_columns = copy.copy(self.entity_columns)
        fv.projection = copy.copy(self.projection)
        return fv

    def __eq__(self, other):
        if not isinstance(other, FeatureView):
            raise TypeError(
                "Comparisons should only involve FeatureView class objects."
            )

        if not super().__eq__(other):
            return False

        if (
            sorted(self.entities) != sorted(other.entities)
            or self.ttl != other.ttl
            or self.online != other.online
            or self.batch_source != other.batch_source
            or self.stream_source != other.stream_source
            or sorted(self.entity_columns) != sorted(other.entity_columns)
        ):
            return False

        return True

    @property
    def join_keys(self) -> List[str]:
        """Returns a list of all the join keys."""
        return [entity.name for entity in self.entity_columns]

    def ensure_valid(self):
        """
        Validates the state of this feature view locally.

        Raises:
            ValueError: The feature view does not have a name or does not have entities.
        """
        super().ensure_valid()

        if not self.entities:
            raise ValueError("Feature view has no entities.")

    @property
    def proto_class(self) -> Type[FeatureViewProto]:
        return FeatureViewProto

    def with_join_key_map(self, join_key_map: Dict[str, str]):
        """
        Returns a copy of this feature view with the join key map set to the given map.
        This join_key mapping operation is only used as part of query operations and will
        not modify the underlying FeatureView.

        Args:
            join_key_map: A map of join keys in which the left is the join_key that
                corresponds with the feature data and the right corresponds with the entity data.

        Examples:
            Join a location feature data table to both the origin column and destination
            column of the entity data.

            temperatures_feature_service = FeatureService(
                name="temperatures",
                features=[
                    location_stats_feature_view
                        .with_name("origin_stats")
                        .with_join_key_map(
                            {"location_id": "origin_id"}
                        ),
                    location_stats_feature_view
                        .with_name("destination_stats")
                        .with_join_key_map(
                            {"location_id": "destination_id"}
                        ),
                ],
            )
        """
        cp = self.__copy__()
        cp.projection.join_key_map = join_key_map

        return cp

    def to_proto(self) -> FeatureViewProto:
        """
        Converts a feature view object to its protobuf representation.

        Returns:
            A FeatureViewProto protobuf.
        """
        meta = self.to_proto_meta()
        ttl_duration = self.get_ttl_duration()

        batch_source_proto = self.batch_source.to_proto()
        batch_source_proto.data_source_class_type = f"{self.batch_source.__class__.__module__}.{self.batch_source.__class__.__name__}"

        stream_source_proto = None
        if self.stream_source:
            stream_source_proto = self.stream_source.to_proto()
            stream_source_proto.data_source_class_type = f"{self.stream_source.__class__.__module__}.{self.stream_source.__class__.__name__}"

        spec = FeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            entity_columns=[field.to_proto() for field in self.entity_columns],
            features=[field.to_proto() for field in self.features],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            ttl=(ttl_duration if ttl_duration is not None else None),
            online=self.online,
            batch_source=batch_source_proto,
            stream_source=stream_source_proto,
        )

        return FeatureViewProto(spec=spec, meta=meta)

    def to_proto_meta(self):
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
        return meta

    def get_ttl_duration(self):
        ttl_duration = None
        if self.ttl is not None:
            ttl_duration = Duration()
            ttl_duration.FromTimedelta(self.ttl)
        return ttl_duration

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
            description=feature_view_proto.spec.description,
            tags=dict(feature_view_proto.spec.tags),
            owner=feature_view_proto.spec.owner,
            online=feature_view_proto.spec.online,
            ttl=(
                timedelta(days=0)
                if feature_view_proto.spec.ttl.ToNanoseconds() == 0
                else feature_view_proto.spec.ttl.ToTimedelta()
            ),
            source=batch_source,
        )
        if stream_source:
            feature_view.stream_source = stream_source

        # This avoids the deprecation warning.
        feature_view.entities = feature_view_proto.spec.entities

        # Instead of passing in a schema, we set the features and entity columns.
        feature_view.features = [
            Field.from_proto(field_proto)
            for field_proto in feature_view_proto.spec.features
        ]
        feature_view.entity_columns = [
            Field.from_proto(field_proto)
            for field_proto in feature_view_proto.spec.entity_columns
        ]

        if len(feature_view.entities) != len(feature_view.entity_columns):
            warnings.warn(
                f"There are some mismatches in your feature view's registered entities. Please check if you have applied your entities correctly."
                f"Entities: {feature_view.entities} vs Entity Columns: {feature_view.entity_columns}"
            )

        # FeatureViewProjections are not saved in the FeatureView proto.
        # Create the default projection.
        feature_view.projection = FeatureViewProjection.from_definition(feature_view)

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
