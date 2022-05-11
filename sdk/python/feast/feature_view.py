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
from typing import Dict, List, Optional, Tuple, Type, Union

from google.protobuf.duration_pb2 import Duration

from feast import utils
from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource, KafkaSource, KinesisSource, PushSource
from feast.entity import Entity
from feast.feature import Feature
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
from feast.usage import log_exceptions
from feast.value_type import ValueType

warnings.simplefilter("once", DeprecationWarning)

# DUMMY_ENTITY is a placeholder entity used in entityless FeatureViews
DUMMY_ENTITY_ID = "__dummy_id"
DUMMY_ENTITY_NAME = "__dummy"
DUMMY_ENTITY_VAL = ""
DUMMY_ENTITY = Entity(
    name=DUMMY_ENTITY_NAME, join_keys=[DUMMY_ENTITY_ID], value_type=ValueType.STRING,
)


class FeatureView(BaseFeatureView):
    """
    A FeatureView defines a logical group of features.

    Attributes:
        name: The unique name of the feature view.
        entities: The list of entities with which this group of features is associated.
        ttl: The amount of time this group of features lives. A ttl of 0 indicates that
            this group of features lives forever. Note that large ttl's or a ttl of 0
            can result in extremely computationally intensive queries.
        batch_source (optional): The batch source of data where this group of features
            is stored. This is optional ONLY if a push source is specified as the
            stream_source, since push sources contain their own batch sources. This is deprecated in favor of `source`.
        stream_source (optional): The stream source of data where this group of features
            is stored. This is deprecated in favor of `source`.
        schema: The schema of the feature view, including feature, timestamp, and entity
            columns.
        features: The list of features defined as part of this feature view. Each
            feature should also be included in the schema.
        online: A boolean indicating whether online retrieval is enabled for this feature
            view.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the feature view, typically the email of the primary
            maintainer.
        source (optional): The source of data for this group of features. May be a stream source, or a batch source.
            If a stream source, the source should contain a batch_source for backfills & batch materialization.
    """

    name: str
    entities: List[str]
    ttl: Optional[timedelta]
    batch_source: DataSource
    stream_source: Optional[DataSource]
    schema: List[Field]
    features: List[Field]
    online: bool
    description: str
    tags: Dict[str, str]
    owner: str
    materialization_intervals: List[Tuple[datetime, datetime]]
    source: Optional[DataSource]

    @log_exceptions
    def __init__(
        self,
        *args,
        name: Optional[str] = None,
        entities: Optional[Union[List[Entity], List[str]]] = None,
        ttl: Optional[Union[Duration, timedelta]] = None,
        batch_source: Optional[DataSource] = None,
        stream_source: Optional[DataSource] = None,
        features: Optional[List[Feature]] = None,
        tags: Optional[Dict[str, str]] = None,
        online: bool = True,
        description: str = "",
        owner: str = "",
        schema: Optional[List[Field]] = None,
        source: Optional[DataSource] = None,
    ):
        """
        Creates a FeatureView object.

        Args:
            name: The unique name of the feature view.
            entities: The list of entities with which this group of features is associated.
            ttl: The amount of time this group of features lives. A ttl of 0 indicates that
                this group of features lives forever. Note that large ttl's or a ttl of 0
                can result in extremely computationally intensive queries.
            batch_source: The batch source of data where this group of features is stored.
            stream_source (optional): The stream source of data where this group of features
                is stored.
            features (deprecated): The list of features defined as part of this feature view.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            online (optional): A boolean indicating whether online retrieval is enabled for
                this feature view.
            description (optional): A human-readable description.
            owner (optional): The owner of the feature view, typically the email of the
                primary maintainer.
            schema (optional): The schema of the feature view, including feature, timestamp,
                and entity columns.
            source (optional): The source of data for this group of features. May be a stream source, or a batch source.
                If a stream source, the source should contain a batch_source for backfills & batch materialization.

        Raises:
            ValueError: A field mapping conflicts with an Entity or a Feature.
        """

        positional_attributes = ["name", "entities", "ttl"]

        _name = name
        _entities = entities
        _ttl = ttl

        if args:
            warnings.warn(
                (
                    "feature view parameters should be specified as a keyword argument instead of a positional arg."
                    "Feast 0.23+ will not support positional arguments to construct feature views"
                ),
                DeprecationWarning,
            )
            if len(args) > len(positional_attributes):
                raise ValueError(
                    f"Only {', '.join(positional_attributes)} are allowed as positional args when defining "
                    f"feature views, for backwards compatibility."
                )
            if len(args) >= 1:
                _name = args[0]
            if len(args) >= 2:
                _entities = args[1]
            if len(args) >= 3:
                _ttl = args[2]

        if not _name:
            raise ValueError("feature view name needs to be specified")

        self.name = _name
        self.entities = (
            [e.name if isinstance(e, Entity) else e for e in _entities]
            if _entities
            else [DUMMY_ENTITY_NAME]
        )

        self._initialize_sources(_name, batch_source, stream_source, source)

        if isinstance(_ttl, Duration):
            self.ttl = timedelta(seconds=int(_ttl.seconds))
            warnings.warn(
                (
                    "The option to pass a Duration object to the ttl parameter is being deprecated. "
                    "Please pass a timedelta object instead. Feast 0.21 and onwards will not support "
                    "Duration objects."
                ),
                DeprecationWarning,
            )
        elif isinstance(_ttl, timedelta) or _ttl is None:
            self.ttl = _ttl
        else:
            raise ValueError(f"unknown value type specified for ttl {type(_ttl)}")

        if features is not None:
            warnings.warn(
                (
                    "The `features` parameter is being deprecated in favor of the `schema` parameter. "
                    "Please switch from using `features` to `schema`. This will also requiring switching "
                    "feature definitions from using `Feature` to `Field`. Feast 0.21 and onwards will not "
                    "support the `features` parameter."
                ),
                DeprecationWarning,
            )

        _schema = schema or []
        if len(_schema) == 0 and features is not None:
            _schema = [Field.from_feature(feature) for feature in features]
        self.schema = _schema

        # TODO(felixwang9817): Infer which fields in the schema are features, timestamps,
        # and entities. For right now we assume that all fields are features, since the
        # current `features` parameter only accepts feature columns.
        _features = _schema

        cols = [entity for entity in self.entities] + [
            field.name for field in _features
        ]
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
            name=_name,
            features=_features,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.online = online
        self.materialization_intervals = []

    def _initialize_sources(self, name, batch_source, stream_source, source):
        if source:
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
                self.stream_source = stream_source
                self.batch_source = source
        else:
            warnings.warn(
                "batch_source and stream_source have been deprecated in favor of `source`."
                "The deprecated fields will be removed in Feast 0.23.",
                DeprecationWarning,
            )
            if stream_source is not None and isinstance(stream_source, PushSource):
                self.stream_source = stream_source
                self.batch_source = stream_source.batch_source
            else:
                if batch_source is None:
                    raise ValueError(
                        f"A batch_source needs to be specified for feature view `{name}`"
                    )
                self.stream_source = stream_source
                self.batch_source = batch_source
        self.source = source

    def __hash__(self):
        return super().__hash__()

    def __copy__(self):
        fv = FeatureView(
            name=self.name,
            entities=self.entities,
            ttl=self.ttl,
            source=self.batch_source,
            stream_source=self.stream_source,
            schema=self.schema,
            tags=self.tags,
            online=self.online,
        )
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
            or self.schema != other.schema
        ):
            return False

        return True

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
            features=[field.to_proto() for field in self.schema],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
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
            schema=[
                Field.from_proto(field_proto)
                for field_proto in feature_view_proto.spec.features
            ],
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
