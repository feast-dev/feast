from typing import TYPE_CHECKING, Dict, List, Optional

from attr import dataclass

from feast.field import Field
from feast.protos.feast.core.FeatureViewProjection_pb2 import (
    FeatureViewProjection as FeatureViewProjectionProto,
)

if TYPE_CHECKING:
    from feast.base_feature_view import BaseFeatureView


@dataclass
class FeatureViewProjection:
    """
    A feature view projection represents a selection of one or more features from a
    single feature view.

    Attributes:
        name: The unique name of the feature view from which this projection is created.
        name_alias: An optional alias for the name.
        features: The list of features represented by the feature view projection.
        join_key_map: A map to modify join key columns during retrieval of this feature
            view projection.
    """

    name: str
    name_alias: Optional[str]
    desired_features: List[str]
    features: List[Field]
    join_key_map: Dict[str, str] = {}

    def name_to_use(self):
        return self.name_alias or self.name

    def to_proto(self) -> FeatureViewProjectionProto:
        feature_reference_proto = FeatureViewProjectionProto(
            feature_view_name=self.name,
            feature_view_name_alias=self.name_alias or "",
            join_key_map=self.join_key_map,
        )
        for feature in self.features:
            feature_reference_proto.feature_columns.append(feature.to_proto())

        return feature_reference_proto

    @staticmethod
    def from_proto(proto: FeatureViewProjectionProto):
        feature_view_projection = FeatureViewProjection(
            name=proto.feature_view_name,
            name_alias=proto.feature_view_name_alias,
            features=[],
            join_key_map=dict(proto.join_key_map),
            desired_features=[],
        )
        for feature_column in proto.feature_columns:
            feature_view_projection.features.append(Field.from_proto(feature_column))

        return feature_view_projection

    @staticmethod
    def from_definition(base_feature_view: "BaseFeatureView"):
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
