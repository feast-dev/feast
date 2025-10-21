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
import logging
import warnings
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Type, Union

from google.protobuf.duration_pb2 import Duration
from google.protobuf.message import Message
from typeguard import typechecked

from feast import utils
from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource, KafkaSource, KinesisSource, PushSource
from feast.entity import Entity
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
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
from feast.protos.feast.core.Transformation_pb2 import (
    FeatureTransformationV2 as FeatureTransformationProto,
)
from feast.types import from_value_type
from feast.value_type import ValueType

warnings.simplefilter("ignore", DeprecationWarning)

# DUMMY_ENTITY is a placeholder entity used in entityless FeatureViews
DUMMY_ENTITY_ID = "__dummy_id"
DUMMY_ENTITY_NAME = "__dummy"
DUMMY_ENTITY_VAL = ""
DUMMY_ENTITY = Entity(
    name=DUMMY_ENTITY_NAME,
    join_keys=[DUMMY_ENTITY_ID],
    value_type=ValueType.UNKNOWN,
)
DUMMY_ENTITY_FIELD = Field(
    name=DUMMY_ENTITY_ID,
    dtype=from_value_type(ValueType.STRING),
)

ONLINE_STORE_TAG_SUFFIX = "online_store_"

logger = logging.getLogger(__name__)


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
    source_views: Optional[List["FeatureView"]]
    entity_columns: List[Field]
    features: List[Field]
    online: bool
    offline: bool
    description: str
    tags: Dict[str, str]
    owner: str
    materialization_intervals: List[Tuple[datetime, datetime]]

    def __init__(
        self,
        *,
        name: str,
        source: Union[DataSource, "FeatureView", List["FeatureView"]],
        sink_source: Optional[DataSource] = None,
        schema: Optional[List[Field]] = None,
        entities: Optional[List[Entity]] = None,
        ttl: Optional[timedelta] = timedelta(days=0),
        online: bool = True,
        offline: bool = False,
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
            offline (optional): A boolean indicating whether write to offline store is enabled for
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
        self.ttl = ttl if ttl else timedelta(days=0)

        # FeatureView is destructive of some of its original arguments,
        # making it impossible to convert idempotently to another format.
        # store these arguments to recover them in conversions.
        self.original_schema = schema
        self.original_entities: List[Entity] = entities or []

        schema = schema or []

        # Normalize source
        self.stream_source = None
        self.data_source: Optional[DataSource] = None
        self.source_views: List[FeatureView] = []

        if isinstance(source, DataSource):
            self.data_source = source
        elif isinstance(source, FeatureView):
            self.source_views = [source]
        elif isinstance(source, list) and all(
            isinstance(sv, FeatureView) for sv in source
        ):
            self.source_views = source
        else:
            raise TypeError(
                "source must be a DataSource, a FeatureView, or a list of FeatureView."
            )

        # Set up stream, batch and derived view sources
        if (
            isinstance(self.data_source, PushSource)
            or isinstance(self.data_source, KafkaSource)
            or isinstance(self.data_source, KinesisSource)
        ):
            # Stream source definition
            self.stream_source = self.data_source
            if not self.data_source.batch_source:
                raise ValueError(
                    f"A batch_source needs to be specified for stream source `{self.data_source.name}`"
                )
            self.batch_source = self.data_source.batch_source
        elif self.data_source:
            # Batch source definition
            self.batch_source = self.data_source
        else:
            # Derived view source definition
            if not sink_source:
                raise ValueError("Derived FeatureView must specify `sink_source`.")
            self.batch_source = sink_source

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

        for field in schema:
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

        assert len([f for f in features if f.vector_index]) < 2, (
            f"Only one vector feature is allowed per feature view. Please update {self.name}."
        )

        # TODO(felixwang9817): Add more robust validation of features.
        if self.batch_source is not None:
            cols = [field.name for field in schema]
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
            source=self.batch_source,
        )
        self.online = online
        self.offline = offline
        self.materialization_intervals = []

    def __hash__(self):
        return super().__hash__()

    def __copy__(self):
        fv = FeatureView(
            name=self.name,
            ttl=self.ttl,
            source=self.source_views
            if self.source_views
            else (self.stream_source if self.stream_source else self.batch_source),
            schema=self.schema,
            tags=self.tags,
            online=self.online,
            offline=self.offline,
            sink_source=self.batch_source if self.source_views else None,
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
            or self.offline != other.offline
            or self.batch_source != other.batch_source
            or self.stream_source != other.stream_source
            or sorted(self.entity_columns) != sorted(other.entity_columns)
            or self.source_views != other.source_views
            or self.materialization_intervals != other.materialization_intervals
        ):
            return False

        if isinstance(self.original_entities, List) and isinstance(
            other.original_entities, List
        ):
            if len(self.original_entities) != len(other.original_entities):
                return False

            for entity1, entity2 in zip(
                self.original_entities, other.original_entities
            ):
                if entity1 != entity2:
                    return False

        return True

    @property
    def join_keys(self) -> List[str]:
        """Returns a list of all the join keys."""
        return [entity.name for entity in self.entity_columns]

    @property
    def schema(self) -> List[Field]:
        return list(set(self.entity_columns + self.features))

    def ensure_valid(self):
        """
        Validates the state of this feature view locally.

        Raises:
            ValueError: The feature view does not have a name or does not have entities.
        """
        super().ensure_valid()

        if self.ttl < timedelta(days=0):
            raise ValueError(
                "Feature view ttl cannot be negative. Please set a positive value or 0 for no ttl."
            )

        if not self.entities:
            raise ValueError("Feature view has no entities.")

    @property
    def proto_class(self) -> Type[Message]:
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

    def update_materialization_intervals(
        self, existing_materialization_intervals: List[Tuple[datetime, datetime]]
    ):
        if (
            len(existing_materialization_intervals) > 0
            and len(self.materialization_intervals) == 0
        ):
            for interval in existing_materialization_intervals:
                self.materialization_intervals.append((interval[0], interval[1]))

    def to_proto(self) -> FeatureViewProto:
        """
        Converts a feature view object to its protobuf representation.

        Returns:
            A FeatureViewProto protobuf.
        """
        return self._to_proto_internal(seen={})

    def _to_proto_internal(
        self, seen: Dict[str, Union[None, FeatureViewProto]]
    ) -> FeatureViewProto:
        if self.name in seen:
            if seen[self.name] is None:
                raise ValueError(
                    f"Cycle detected during serialization of FeatureView: {self.name}"
                )
            return seen[self.name]  # type: ignore[return-value]

        seen[self.name] = None

        spec = self.to_proto_spec(seen)
        meta = self.to_proto_meta()
        proto = FeatureViewProto(spec=spec, meta=meta)
        seen[self.name] = proto
        return proto

    def to_proto_spec(
        self, seen: Dict[str, Union[None, FeatureViewProto]]
    ) -> FeatureViewSpecProto:
        ttl_duration = self.get_ttl_duration()

        batch_source_proto = None
        if self.batch_source:
            batch_source_proto = self.batch_source.to_proto()
            batch_source_proto.data_source_class_type = f"{self.batch_source.__class__.__module__}.{self.batch_source.__class__.__name__}"

        stream_source_proto = None
        if self.stream_source:
            stream_source_proto = self.stream_source.to_proto()
            stream_source_proto.data_source_class_type = f"{self.stream_source.__class__.__module__}.{self.stream_source.__class__.__name__}"
        source_view_protos = None
        if self.source_views:
            source_view_protos = [
                view._to_proto_internal(seen).spec for view in self.source_views
            ]

        feature_transformation_proto = None
        if hasattr(self, "feature_transformation") and self.feature_transformation:
            from feast.protos.feast.core.Transformation_pb2 import (
                SubstraitTransformationV2 as SubstraitTransformationProto,
            )
            from feast.protos.feast.core.Transformation_pb2 import (
                UserDefinedFunctionV2 as UserDefinedFunctionProto,
            )

            transformation_proto = self.feature_transformation.to_proto()

            if isinstance(transformation_proto, UserDefinedFunctionProto):
                feature_transformation_proto = FeatureTransformationProto(
                    user_defined_function=transformation_proto,
                )
            elif isinstance(transformation_proto, SubstraitTransformationProto):
                feature_transformation_proto = FeatureTransformationProto(
                    substrait_transformation=transformation_proto,
                )

        original_entities: List[EntityProto] = []
        for entity in self.original_entities:
            original_entities.append(entity.to_proto())

        return FeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            entity_columns=[field.to_proto() for field in self.entity_columns],
            features=[feature.to_proto() for feature in self.features],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            ttl=(ttl_duration if ttl_duration is not None else None),
            online=self.online,
            offline=self.offline,
            batch_source=batch_source_proto,
            stream_source=stream_source_proto,
            source_views=source_view_protos,
            original_entities=original_entities,
            feature_transformation=feature_transformation_proto,
        )

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

    def is_update_compatible_with(self, updated) -> Tuple[bool, List[str]]:
        """
        Checks if updating this FeatureView to 'updated' is compatible.
        Returns a tuple:
            (True, []) if compatible;
            (False, [reasons...]) otherwise.
        """
        reasons: List[str] = []
        old_fields = {f.name: f.dtype for f in self.schema}
        new_fields = {f.name: f.dtype for f in updated.schema}

        # TODO: Think about how the following check should be handled for stream FVs
        if self.materialization_intervals:
            if updated.entities != self.entities:
                reasons.append(
                    f"entity definitions cannot change for FeatureView: {self.name}"
                )

            removed = old_fields.keys() - new_fields.keys()
            for fname in sorted(removed):
                if fname in self.entities:
                    reasons.append(
                        f"feature '{fname}' removed from FeatureView '{self.name}' is an entity key and cannot be removed"
                    )
                else:
                    logger.info(
                        "Feature '%s' removed from FeatureView '%s'.", fname, self.name
                    )
        else:
            logger.info("No materialization intervals: skipping entity related checks.")
        for fname, old_dtype in old_fields.items():
            if fname in new_fields and new_fields[fname] != old_dtype:
                logger.warning(
                    f"feature '{fname}' type changed ({old_dtype} to {new_fields[fname]}), this is "
                    "generally an illegal operation, please re-materialize all data in order for this "
                    f"change to reflect correctly for this feature view {self.name}."
                )

        return len(reasons) == 0, reasons

    @classmethod
    def from_proto(cls, feature_view_proto: FeatureViewProto) -> "FeatureView":
        return cls._from_proto_internal(feature_view_proto, seen={})

    @classmethod
    def _from_proto_internal(
        cls,
        feature_view_proto: FeatureViewProto,
        seen: Dict[str, Union[None, "FeatureView"]],
    ) -> "FeatureView":
        """
        Creates a feature view from a protobuf representation of a feature view.

        Args:
            feature_view_proto: A protobuf representation of a feature view.
            seen: A dictionary to keep track of already seen feature views to avoid recursion.

        Returns:
            A FeatureViewProto object based on the feature view protobuf.
        """
        feature_view_name = feature_view_proto.spec.name

        if feature_view_name in seen:
            if seen[feature_view_name] is None:
                raise ValueError(
                    f"Cycle detected while deserializing FeatureView: {feature_view_name}"
                )
            return seen[feature_view_name]  # type: ignore[return-value]
        seen[feature_view_name] = None

        batch_source = (
            DataSource.from_proto(feature_view_proto.spec.batch_source)
            if feature_view_proto.spec.HasField("batch_source")
            else None
        )
        stream_source = (
            DataSource.from_proto(feature_view_proto.spec.stream_source)
            if feature_view_proto.spec.HasField("stream_source")
            else None
        )
        source_views = [
            FeatureView._from_proto_internal(
                FeatureViewProto(spec=view_spec, meta=None), seen
            )
            for view_spec in feature_view_proto.spec.source_views
        ]

        has_transformation = feature_view_proto.spec.HasField("feature_transformation")

        if has_transformation and cls == FeatureView:
            from feast.batch_feature_view import BatchFeatureView
            from feast.transformation.python_transformation import PythonTransformation
            from feast.transformation.substrait_transformation import (
                SubstraitTransformation,
            )

            feature_transformation_proto = (
                feature_view_proto.spec.feature_transformation
            )
            transformation = None

            if feature_transformation_proto.HasField("user_defined_function"):
                transformation = PythonTransformation.from_proto(
                    feature_transformation_proto.user_defined_function
                )
            elif feature_transformation_proto.HasField("substrait_transformation"):
                transformation = SubstraitTransformation.from_proto(
                    feature_transformation_proto.substrait_transformation
                )

            feature_view: FeatureView = BatchFeatureView(  # type: ignore[assignment]
                name=feature_view_proto.spec.name,
                description=feature_view_proto.spec.description,
                tags=dict(feature_view_proto.spec.tags),
                owner=feature_view_proto.spec.owner,
                online=feature_view_proto.spec.online,
                offline=feature_view_proto.spec.offline,
                ttl=(
                    timedelta(days=0)
                    if feature_view_proto.spec.ttl.ToNanoseconds() == 0
                    else feature_view_proto.spec.ttl.ToTimedelta()
                ),
                source=source_views if source_views else batch_source,  # type: ignore[arg-type]
                sink_source=batch_source if source_views else None,
                feature_transformation=transformation,
            )
        else:
            feature_view = cls(  # type: ignore[assignment]
                name=feature_view_proto.spec.name,
                description=feature_view_proto.spec.description,
                tags=dict(feature_view_proto.spec.tags),
                owner=feature_view_proto.spec.owner,
                online=feature_view_proto.spec.online,
                offline=feature_view_proto.spec.offline,
                ttl=(
                    timedelta(days=0)
                    if feature_view_proto.spec.ttl.ToNanoseconds() == 0
                    else feature_view_proto.spec.ttl.ToTimedelta()
                ),
                source=source_views if source_views else batch_source,
                sink_source=batch_source if source_views else None,
            )
        if stream_source:
            feature_view.stream_source = stream_source

        # This avoids the deprecation warning.
        feature_view.entities = list(feature_view_proto.spec.entities)
        feature_view.original_entities = [
            Entity.from_proto(entity)
            for entity in feature_view_proto.spec.original_entities
        ]

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
                f"There are some mismatches in your feature view: {feature_view.name} registered entities. Please check if you have applied your entities correctly."
                f"Entities: {feature_view.entities} vs Entity Columns: {feature_view.entity_columns}"
            )

        feature_view.original_schema = (
            feature_view.entity_columns + feature_view.features
        )

        # FeatureViewProjections are not saved in the FeatureView proto.
        # Create the default projection.
        feature_view.projection = FeatureViewProjection.from_feature_view_definition(
            feature_view
        )

        if feature_view_proto.meta.HasField("created_timestamp"):
            feature_view.created_timestamp = (
                feature_view_proto.meta.created_timestamp.ToDatetime(
                    tzinfo=timezone.utc  # type: ignore[call-arg]
                )
            )
        if feature_view_proto.meta.HasField("last_updated_timestamp"):
            feature_view.last_updated_timestamp = (
                feature_view_proto.meta.last_updated_timestamp.ToDatetime(
                    tzinfo=timezone.utc  # type: ignore[call-arg]
                )
            )

        for interval in feature_view_proto.meta.materialization_intervals:
            feature_view.materialization_intervals.append(
                (
                    utils.make_tzaware(
                        interval.start_time.ToDatetime(tzinfo=timezone.utc)  # type: ignore[call-arg]
                    ),
                    utils.make_tzaware(
                        interval.end_time.ToDatetime(tzinfo=timezone.utc)  # type: ignore[call-arg]
                    ),
                )
            )

        seen[feature_view_name] = feature_view
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

    @property
    def get_online_store_tags(self) -> Dict[str, str]:
        """
        Retrieves online store specific tags which are prefixed with online_store_.
        This helps to identify the overrides provided at feature view level. Not all
        online store configurations are relevant for override.

        Returns:
            A dictionary of tags. If no tags are found, returns an empty dictionary.
        """
        tags = {
            k.removeprefix(ONLINE_STORE_TAG_SUFFIX).lower(): v
            for k, v in self.tags.items()
            if k.lower().startswith(ONLINE_STORE_TAG_SUFFIX)
        }
        return tags
