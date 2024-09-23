from typing import TYPE_CHECKING, Dict, List, Optional

from attr import dataclass

from feast.data_source import DataSource
from feast.field import Field
from feast.protos.feast.core.FeatureViewProjection_pb2 import (
    FeatureViewProjection as FeatureViewProjectionProto,
)

if TYPE_CHECKING:
    from feast.base_feature_view import BaseFeatureView
    from feast.feature_view import FeatureView


@dataclass
class FeatureViewProjection:
    """
    A feature view projection represents a selection of one or more features from a
    single feature view.

    Attributes:
        name: The unique name of the feature view from which this projection is created.
        name_alias: An optional alias for the name.
        features: The list of features represented by the feature view projection.
        desired_features: The list of features that this feature view projection intends to select.
            If empty, the projection intends to select all features. This attribute is only used
            for feature service inference. It should only be set if the underlying feature view
            is not ready to be projected, i.e. still needs to go through feature inference.
        join_key_map: A map to modify join key columns during retrieval of this feature
            view projection.
        timestamp_field: The timestamp field of the feature view projection.
        date_partition_column: The date partition column of the feature view projection.
        created_timestamp_column: The created timestamp column of the feature view projection.
        batch_source: The batch source of data where this group of features
            is stored. This is optional ONLY if a push source is specified as the
            stream_source, since push sources contain their own batch sources.

    """

    name: str
    name_alias: Optional[str]
    desired_features: List[str]
    features: List[Field]
    join_key_map: Dict[str, str] = {}
    timestamp_field: Optional[str] = None
    date_partition_column: Optional[str] = None
    created_timestamp_column: Optional[str] = None
    batch_source: Optional[DataSource] = None

    def name_to_use(self):
        return self.name_alias or self.name

    def to_proto(self) -> FeatureViewProjectionProto:
        batch_source = None
        if getattr(self, "batch_source", None):
            if isinstance(self.batch_source, DataSource):
                batch_source = self.batch_source.to_proto()
            else:
                batch_source = self.batch_source
        feature_reference_proto = FeatureViewProjectionProto(
            feature_view_name=self.name,
            feature_view_name_alias=self.name_alias or "",
            join_key_map=self.join_key_map,
            timestamp_field=self.timestamp_field or "",
            date_partition_column=self.date_partition_column or "",
            created_timestamp_column=self.created_timestamp_column or "",
            batch_source=batch_source,
        )
        for feature in self.features:
            feature_reference_proto.feature_columns.append(feature.to_proto())

        return feature_reference_proto

    @staticmethod
    def from_proto(proto: FeatureViewProjectionProto) -> "FeatureViewProjection":
        batch_source = (
            DataSource.from_proto(proto.batch_source)
            if str(getattr(proto, "batch_source"))
            else None
        )
        feature_view_projection = FeatureViewProjection(
            name=proto.feature_view_name,
            name_alias=proto.feature_view_name_alias or None,
            features=[],
            join_key_map=dict(proto.join_key_map),
            desired_features=[],
            timestamp_field=proto.timestamp_field or None,
            date_partition_column=proto.date_partition_column or None,
            created_timestamp_column=proto.created_timestamp_column or None,
            batch_source=batch_source,
        )
        for feature_column in proto.feature_columns:
            feature_view_projection.features.append(Field.from_proto(feature_column))

        return feature_view_projection

    @staticmethod
    def from_feature_view_definition(feature_view: "FeatureView"):
        # TODO need to implement this for StreamFeatureViews
        if getattr(feature_view, "batch_source", None):
            return FeatureViewProjection(
                name=feature_view.name,
                name_alias=None,
                features=feature_view.features,
                desired_features=[],
                timestamp_field=feature_view.batch_source.created_timestamp_column
                or None,
                created_timestamp_column=feature_view.batch_source.created_timestamp_column
                or None,
                date_partition_column=feature_view.batch_source.date_partition_column
                or None,
                batch_source=feature_view.batch_source or None,
            )
        else:
            return FeatureViewProjection(
                name=feature_view.name,
                name_alias=None,
                features=feature_view.features,
                desired_features=[],
            )

    @staticmethod
    def from_definition(base_feature_view: "BaseFeatureView"):
        if getattr(base_feature_view, "batch_source", None):
            return FeatureViewProjection(
                name=base_feature_view.name,
                name_alias=None,
                features=base_feature_view.features,
                desired_features=[],
                timestamp_field=base_feature_view.batch_source.created_timestamp_column  # type:ignore[attr-defined]
                or None,
                created_timestamp_column=base_feature_view.batch_source.created_timestamp_column  # type:ignore[attr-defined]
                or None,
                date_partition_column=base_feature_view.batch_source.date_partition_column  # type:ignore[attr-defined]
                or None,
                batch_source=base_feature_view.batch_source or None,  # type:ignore[attr-defined]
            )
        else:
            return FeatureViewProjection(
                name=base_feature_view.name,
                name_alias=None,
                features=base_feature_view.features,
                desired_features=[],
            )

    def get_feature(self, feature_name: str) -> Field:
        try:
            return next(field for field in self.features if field.name == feature_name)
        except StopIteration:
            raise KeyError(
                f"Feature {feature_name} not found in projection {self.name_to_use()}"
            )
