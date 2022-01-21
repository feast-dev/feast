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
    FeatureServiceMeta,
    FeatureServiceSpec,
)
from feast.usage import log_exceptions


class FeatureService:
    """
    A feature service is a logical grouping of features for retrieval (training or serving).
    The features grouped by a feature service may come from any number of feature views.

    Args:
        name: Unique name of the feature service.
        features: A list of Features that are grouped as part of this FeatureService.
            The list may contain Feature Views, Feature Tables, or a subset of either.
        tags (optional): A dictionary of key-value pairs used for organizing Feature
            Services.
    """

    _name: str
    _feature_view_projections: List[FeatureViewProjection]
    _tags: Dict[str, str]
    _description: Optional[str] = None
    _created_timestamp: Optional[datetime] = None
    _last_updated_timestamp: Optional[datetime] = None

    @log_exceptions
    def __init__(
        self,
        name: str,
        features: List[Union[FeatureView, OnDemandFeatureView]],
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
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
                    "The FeatureService {fs_name} has been provided with an invalid type"
                    f'{type(feature_grouping)} as part of the "features" argument.)'
                )

        self._tags = tags or {}
        self._description = description
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
        if self.tags != other.tags or self.name != other.name:
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
    def tags(self) -> Dict[str, str]:
        return self._tags

    @tags.setter
    def tags(self, tags: Dict[str, str]):
        self._tags = tags

    @property
    def description(self) -> Optional[str]:
        return self._description

    @description.setter
    def description(self, description: str):
        self._description = description

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

    @staticmethod
    def from_proto(feature_service_proto: FeatureServiceProto):
        """
        Converts a FeatureServiceProto to a FeatureService object.

        Args:
            feature_service_proto: A protobuf representation of a FeatureService.
        """
        fs = FeatureService(
            name=feature_service_proto.spec.name,
            features=[],
            tags=dict(feature_service_proto.spec.tags),
            description=(
                feature_service_proto.spec.description
                if feature_service_proto.spec.description != ""
                else None
            ),
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
        Converts a FeatureService to its protobuf representation.

        Returns:
            A FeatureServiceProto protobuf.
        """
        meta = FeatureServiceMeta()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)

        spec = FeatureServiceSpec(
            name=self.name,
            features=[
                projection.to_proto() for projection in self.feature_view_projections
            ],
        )

        if self.tags:
            spec.tags.update(self.tags)
        if self.description:
            spec.description = self.description

        feature_service_proto = FeatureServiceProto(spec=spec, meta=meta)
        return feature_service_proto

    def validate(self):
        pass
