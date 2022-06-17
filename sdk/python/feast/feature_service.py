import warnings
from datetime import datetime
from typing import Dict, List, Optional, Union

from google.protobuf.json_format import MessageToJson
from typeguard import typechecked

from feast.base_feature_view import BaseFeatureView
from feast.feature_logging import LoggingConfig
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


@typechecked
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

    name: str
    _features: List[Union[FeatureView, OnDemandFeatureView]]
    feature_view_projections: List[FeatureViewProjection]
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None
    logging_config: Optional[LoggingConfig] = None

    @log_exceptions
    def __init__(
        self,
        *args,
        name: Optional[str] = None,
        features: Optional[List[Union[FeatureView, OnDemandFeatureView]]] = None,
        tags: Dict[str, str] = None,
        description: str = "",
        owner: str = "",
        logging_config: Optional[LoggingConfig] = None,
    ):
        """
        Creates a FeatureService object.

        Raises:
            ValueError: If one of the specified features is not a valid type.
        """
        positional_attributes = ["name", "features"]
        _name = name
        _features = features
        if args:
            warnings.warn(
                (
                    "Feature service parameters should be specified as a keyword argument instead of a positional arg."
                    "Feast 0.24+ will not support positional arguments to construct feature service"
                ),
                DeprecationWarning,
            )
            if len(args) > len(positional_attributes):
                raise ValueError(
                    f"Only {', '.join(positional_attributes)} are allowed as positional args when defining "
                    f"feature service, for backwards compatibility."
                )
            if len(args) >= 1:
                _name = args[0]
            if len(args) >= 2:
                _features = args[1]

        if not _name:
            raise ValueError("Feature service name needs to be specified")

        if not _features:
            # Technically, legal to create feature service with no feature views before.
            _features = []

        self.name = _name
        self._features = _features
        self.feature_view_projections = []
        self.description = description
        self.tags = tags or {}
        self.owner = owner
        self.created_timestamp = None
        self.last_updated_timestamp = None
        self.logging_config = logging_config
        for feature_grouping in self._features:
            if isinstance(feature_grouping, BaseFeatureView):
                self.feature_view_projections.append(feature_grouping.projection)

    def infer_features(self, fvs_to_update: Optional[Dict[str, FeatureView]] = None):
        for feature_grouping in self._features:
            if isinstance(feature_grouping, BaseFeatureView):
                # For feature services that depend on an unspecified feature view, apply inferred schema
                if fvs_to_update and feature_grouping.name in fvs_to_update:
                    if feature_grouping.projection.desired_features:
                        desired_features = set(
                            feature_grouping.projection.desired_features
                        )
                        actual_features = set(
                            [
                                f.name
                                for f in fvs_to_update[feature_grouping.name].features
                            ]
                        )
                        assert desired_features.issubset(actual_features)
                        # We need to set the features for the projection at this point so we ensure we're starting with
                        # an empty list.
                        feature_grouping.projection.features = []
                        for f in fvs_to_update[feature_grouping.name].features:
                            if f.name in desired_features:
                                feature_grouping.projection.features.append(f)
                    else:
                        feature_grouping.projection.features = fvs_to_update[
                            feature_grouping.name
                        ].features
            else:
                raise ValueError(
                    f"The feature service {self.name} has been provided with an invalid type "
                    f'{type(feature_grouping)} as part of the "features" argument.)'
                )

    def __repr__(self):
        items = (f"{k} = {v}" for k, v in self.__dict__.items())
        return f"<{self.__class__.__name__}({', '.join(items)})>"

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __hash__(self):
        return hash(self.name)

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
            logging_config=LoggingConfig.from_proto(
                feature_service_proto.spec.logging_config
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
            logging_config=self.logging_config.to_proto()
            if self.logging_config
            else None,
        )

        return FeatureServiceProto(spec=spec, meta=meta)

    def validate(self):
        pass
