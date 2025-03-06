import copy
import warnings
from datetime import timedelta
from typing import Dict, List, Optional, Type

from google.protobuf.message import Message
from typeguard import typechecked

from feast import utils
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.protos.feast.core.SortedFeatureView_pb2 import (
    SortedFeatureView as SortedFeatureViewProto,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import (
    SortedFeatureViewSpec as SortedFeatureViewSpecProto,
)
from feast.sort_key import SortKey

warnings.simplefilter("ignore", DeprecationWarning)


@typechecked
class SortedFeatureView(FeatureView):
    """
    SortedFeatureView extends FeatureView by adding support for range queries
    via sort keys.
    """

    sort_keys: List[SortKey]

    def __init__(
        self,
        *,
        name: str,
        source: DataSource,
        schema: Optional[List[Field]] = None,
        entities: Optional[List[Entity]] = None,
        ttl: Optional[timedelta] = timedelta(days=0),
        online: bool = True,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        sort_keys: Optional[List[SortKey]] = None,
        _skip_validation: bool = False,  # only skipping validation for proto creation, internal use only
    ):
        super().__init__(
            name=name,
            source=source,
            schema=schema,
            entities=entities,
            ttl=ttl,
            online=online,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.sort_keys = sort_keys if sort_keys is not None else []
        if not _skip_validation:
            self.ensure_valid()

    def __copy__(self):
        sfv = SortedFeatureView(
            name=self.name,
            source=self.stream_source if self.stream_source else self.batch_source,
            schema=self.schema,
            entities=self.original_entities,
            ttl=self.ttl,
            online=self.online,
            description=self.description,
            tags=copy.deepcopy(self.tags),
            owner=self.owner,
            sort_keys=copy.copy(self.sort_keys),
        )
        sfv.entities = self.entities
        sfv.features = copy.copy(self.features)
        sfv.entity_columns = copy.copy(self.entity_columns)
        sfv.projection = copy.copy(self.projection)
        return sfv

    def __eq__(self, other):
        if not isinstance(other, SortedFeatureView):
            return NotImplemented
        if not super().__eq__(other):
            return False
        # Compare sort_keys lists
        return self.sort_keys == other.sort_keys

    def ensure_valid(self):
        """
        Validates this SortedFeatureView. In addition to the base FeatureView validations.
        """
        super().ensure_valid()

        reserved_columns = {"event_ts", "created_ts", "entity_key"}
        feature_map = {}

        for field in self.features:
            if field.name in reserved_columns:
                raise ValueError(
                    f"Field name '{field.name}' is reserved and cannot be used as a feature name."
                )
            if field.name in self.entities:
                raise ValueError(
                    f"Feature name '{field.name}' is an entity name and cannot be used as a feature."
                )
            if field.name in feature_map:
                raise ValueError(f"Duplicate feature name found: '{field.name}'.")
            feature_map[field.name] = field

        valid_feature_names = list(feature_map.keys())

        if not self.sort_keys:
            raise ValueError(
                "SortedFeatureView must have at least one sort key defined."
            )

        for sort_key in self.sort_keys:
            # Sort keys should not conflict with entity names.
            if sort_key.name in self.entities:
                raise ValueError(
                    f"Sort key '{sort_key.name}' cannot be part of entity columns."
                )

            # Validate that the sort key corresponds to a feature.
            if sort_key.name not in feature_map:
                raise ValueError(
                    f"Sort key '{sort_key.name}' does not match any feature name. "
                    f"Valid options are: {valid_feature_names}"
                )

            expected_value_type = feature_map[sort_key.name].dtype.to_value_type()
            if sort_key.value_type != expected_value_type:
                raise ValueError(
                    f"Sort key '{sort_key.name}' has value type {sort_key.value_type} which does not match "
                    f"the expected feature value type {expected_value_type} for feature '{sort_key.name}'."
                )

    @property
    def proto_class(self) -> Type[Message]:
        return SortedFeatureViewProto

    def to_proto(self):
        """
        Converts this SortedFeatureView to its protobuf representation.
        """
        meta = self.to_proto_meta()
        ttl_duration = self.get_ttl_duration()

        # Convert batch and stream sources.
        batch_source_proto = self.batch_source.to_proto()
        batch_source_proto.data_source_class_type = (
            f"{self.batch_source.__class__.__module__}."
            f"{self.batch_source.__class__.__name__}"
        )

        stream_source_proto = None
        if self.stream_source:
            stream_source_proto = self.stream_source.to_proto()
            stream_source_proto.data_source_class_type = (
                f"{self.stream_source.__class__.__module__}."
                f"{self.stream_source.__class__.__name__}"
            )

        original_entities = [entity.to_proto() for entity in self.original_entities]

        spec = SortedFeatureViewSpecProto(
            name=self.name,
            entities=self.entities,
            features=[field.to_proto() for field in self.features],
            entity_columns=[field.to_proto() for field in self.entity_columns],
            sort_keys=[sk.to_proto() for sk in self.sort_keys],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            ttl=(ttl_duration if ttl_duration is not None else None),
            batch_source=batch_source_proto,
            stream_source=stream_source_proto,
            online=self.online,
            original_entities=original_entities,
        )

        return SortedFeatureViewProto(spec=spec, meta=meta)

    @classmethod
    def from_proto(cls, sfv_proto):
        """
        Creates a SortedFeatureView from its protobuf representation.
        """
        spec = sfv_proto.spec

        batch_source = DataSource.from_proto(spec.batch_source)
        stream_source = (
            DataSource.from_proto(spec.stream_source)
            if spec.HasField("stream_source")
            else None
        )

        # Create the SortedFeatureView instance.
        sorted_feature_view = cls(
            name=spec.name,
            description=spec.description,
            tags=dict(spec.tags),
            owner=spec.owner,
            online=spec.online,
            ttl=(
                timedelta(days=0)
                if spec.ttl.ToNanoseconds() == 0
                else spec.ttl.ToTimedelta()
            ),
            source=batch_source,
            schema=None,
            entities=None,
            sort_keys=[SortKey.from_proto(sk) for sk in spec.sort_keys],
            _skip_validation=True,
        )

        if stream_source:
            sorted_feature_view.stream_source = stream_source

        sorted_feature_view.entities = list(spec.entities)
        sorted_feature_view.original_entities = [
            Entity.from_proto(e) for e in spec.original_entities
        ]
        sorted_feature_view.features = [Field.from_proto(f) for f in spec.features]
        sorted_feature_view.entity_columns = [
            Field.from_proto(f) for f in spec.entity_columns
        ]
        sorted_feature_view.original_schema = (
            sorted_feature_view.entity_columns + sorted_feature_view.features
        )

        sorted_feature_view.projection = FeatureViewProjection.from_definition(
            sorted_feature_view
        )

        if sfv_proto.meta.HasField("created_timestamp"):
            sorted_feature_view.created_timestamp = (
                sfv_proto.meta.created_timestamp.ToDatetime()
            )
        if sfv_proto.meta.HasField("last_updated_timestamp"):
            sorted_feature_view.last_updated_timestamp = (
                sfv_proto.meta.last_updated_timestamp.ToDatetime()
            )
        for interval in sfv_proto.meta.materialization_intervals:
            sorted_feature_view.materialization_intervals.append(
                (
                    utils.make_tzaware(interval.start_time.ToDatetime()),
                    utils.make_tzaware(interval.end_time.ToDatetime()),
                )
            )

        # Run validation after attributes are set
        sorted_feature_view.ensure_valid()

        return sorted_feature_view
