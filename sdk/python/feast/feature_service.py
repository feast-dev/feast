from datetime import datetime
from typing import Dict, List, Optional, Union

from google.protobuf.json_format import MessageToJson

from feast.base_feature_view import BaseFeatureView
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureServiceMeta as FeatureServiceMetaProto,
)
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureServiceSpec as FeatureServiceSpecProto,
)
from feast.usage import log_exceptions


class FeatureService:
    """
    A feature service defines a logical group of features from one or more feature views.
    This group of features can be retrieved together during training or serving.

    Attributes:
        name: The unique name of the feature service.
        feature_view_projections: A list containing feature views and feature view
            projections, representing the features in the feature service.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the feature service, typically the email of the primary
            maintainer.
        created_timestamp: The time when the feature service was created.
        last_updated_timestamp: The time when the feature service was last updated.
    """

    _name: str
    _feature_view_projections: List[FeatureViewProjection]
    _description: str
    _tags: Dict[str, str]
    _owner: str
    _created_timestamp: Optional[datetime] = None
    _last_updated_timestamp: Optional[datetime] = None

    @log_exceptions
    def __init__(
        self,
        name: str,
        features: List[Union[FeatureView, OnDemandFeatureView]],
        tags: Dict[str, str] = None,
        description: str = "",
        owner: str = "",
    ):
        """
        Creates a FeatureService object.

        Raises:
            ValueError: If one of the specified features is not a valid type.
        """
        self._name = name
        self._feature_view_projections = []

        for feature_grouping in features:
            if isinstance(feature_grouping, BaseFeatureView):
                self._feature_view_projections.append(feature_grouping.projection)
            else:
                raise ValueError(
                    f"The feature service {name} has been provided with an invalid type "
                    f'{type(feature_grouping)} as part of the "features" argument.)'
                )

        self._description = description
        self._tags = tags or {}
        self._owner = owner
        self._created_timestamp = None
        self._last_updated_timestamp = None

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash((id(self), self.name))

    def __eq__(self, other):
        if not isinstance(other, FeatureService):
            raise TypeError(
                "Comparisons should only involve FeatureService class objects."
            )

        if (
            self.name != other.name
            or self.description != other.description
            or self.tags != other.tags
            or self.owner != other.owner
        ):
            return False

        if sorted(self.feature_view_projections) != sorted(
            other.feature_view_projections
        ):
            return False

        return True

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def feature_view_projections(self) -> List[FeatureViewProjection]:
        return self._feature_view_projections

    @feature_view_projections.setter
    def feature_view_projections(
        self, feature_view_projections: List[FeatureViewProjection]
    ):
        self._feature_view_projections = feature_view_projections

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str):
        self._description = description

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    @tags.setter
    def tags(self, tags: Dict[str, str]):
        self._tags = tags

    @property
    def owner(self) -> str:
        return self._owner

    @owner.setter
    def owner(self, owner: str):
        self._owner = owner

    @property
    def created_timestamp(self) -> Optional[datetime]:
        return self._created_timestamp

    @created_timestamp.setter
    def created_timestamp(self, created_timestamp: datetime):
        self._created_timestamp = created_timestamp

    @property
    def last_updated_timestamp(self) -> Optional[datetime]:
        return self._last_updated_timestamp

    @last_updated_timestamp.setter
    def last_updated_timestamp(self, last_updated_timestamp: datetime):
        self._last_updated_timestamp = last_updated_timestamp

    @classmethod
    def from_proto(cls, feature_service_proto: FeatureServiceProto):
        """
        Converts a FeatureServiceProto to a FeatureService object.

        Args:
            feature_service_proto: A protobuf representation of a FeatureService.
        """
        fs = cls(
            name=feature_service_proto.spec.name,
            features=[],
            tags=dict(feature_service_proto.spec.tags),
            description=feature_service_proto.spec.description,
            owner=feature_service_proto.spec.owner,
        )
        fs.feature_view_projections.extend(
            [
                FeatureViewProjection.from_proto(projection)
                for projection in feature_service_proto.spec.features
            ]
        )

        if feature_service_proto.meta.HasField("created_timestamp"):
            fs.created_timestamp = (
                feature_service_proto.meta.created_timestamp.ToDatetime()
            )
        if feature_service_proto.meta.HasField("last_updated_timestamp"):
            fs.last_updated_timestamp = (
                feature_service_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return fs

    def to_proto(self) -> FeatureServiceProto:
        """
        Converts a feature service to its protobuf representation.

        Returns:
            A FeatureServiceProto protobuf.
        """
        meta = FeatureServiceMetaProto()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)

        spec = FeatureServiceSpecProto(
            name=self.name,
            features=[
                projection.to_proto() for projection in self.feature_view_projections
            ],
            tags=self.tags,
            description=self.description,
            owner=self.owner,
        )

        return FeatureServiceProto(spec=spec, meta=meta)

    def validate(self):
        pass
