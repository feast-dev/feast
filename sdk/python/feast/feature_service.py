from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from google.protobuf.json_format import MessageToJson
from typeguard import typechecked

from feast.base_feature_view import BaseFeatureView
from feast.errors import (
    FeastObjectNotFoundException,
    FeatureViewMissingDuringFeatureServiceInference,
)
from feast.feature_logging import LoggingConfig
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.field import Field
from feast.labeling.label_view import LabelView
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

if TYPE_CHECKING:
    from feast.infra.registry.base_registry import BaseRegistry


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
    _features: List[Union[FeatureView, OnDemandFeatureView, LabelView]]
    feature_view_projections: List[FeatureViewProjection]
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None
    logging_config: Optional[LoggingConfig] = None

    precompute_online: bool = False

    def __init__(
        self,
        *,
        name: str,
        features: List[Union[FeatureView, OnDemandFeatureView, LabelView]],
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        logging_config: Optional[LoggingConfig] = None,
        precompute_online: bool = False,
    ):
        """
        Creates a FeatureService object.

        Args:
            name: The unique name of the feature service.
            features: A list containing feature views and feature view
                projections, representing the features in the feature service.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the feature view, typically the email of the
                primary maintainer.
            precompute_online (optional): When True, a pre-computed feature vector is
                maintained per entity for single-read online retrieval.
        """
        self.name = name
        self._features = features
        self.feature_view_projections = []
        self.description = description
        self.tags = tags or {}
        self.owner = owner
        self.created_timestamp = None
        self.last_updated_timestamp = None
        self.logging_config = logging_config
        self.precompute_online = precompute_online
        for feature_grouping in self._features:
            if isinstance(feature_grouping, BaseFeatureView):
                self.feature_view_projections.append(feature_grouping.projection)

    def infer_features(
        self, fvs_to_update: Dict[str, Union[FeatureView, BaseFeatureView]]
    ):
        """
        Infers the features for the projections of this feature service, and updates this feature
        service in place.

        This method is necessary since feature services may rely on feature views which require
        feature inference.

        Args:
            fvs_to_update: A mapping of feature view names to corresponding feature views that
                contains all the feature views necessary to run inference.
        """
        for feature_grouping in self._features:
            if isinstance(feature_grouping, BaseFeatureView):
                projection = feature_grouping.projection

                if projection.desired_features:
                    # The projection wants to select a specific set of inferred features.
                    # Example: FeatureService(features=[fv[["inferred_feature"]]]), where
                    # 'fv' is a feature view that was defined without a schema.
                    if feature_grouping.name in fvs_to_update:
                        # First we validate that the selected features have actually been inferred.
                        desired_features = set(projection.desired_features)
                        actual_features = set(
                            [
                                f.name
                                for f in fvs_to_update[feature_grouping.name].features
                            ]
                        )
                        assert desired_features.issubset(actual_features)

                        # Then we extract the selected features and add them to the projection.
                        projection.features = []
                        for f in fvs_to_update[feature_grouping.name].features:
                            if f.name in desired_features:
                                projection.features.append(f)
                    else:
                        raise FeatureViewMissingDuringFeatureServiceInference(
                            feature_view_name=feature_grouping.name,
                            feature_service_name=self.name,
                        )

                    continue

                if projection.features:
                    # The projection has already selected features from a feature view with a
                    # known schema, so no action needs to be taken.
                    # Example: FeatureService(features=[fv[["existing_feature"]]]), where
                    # 'existing_feature' was defined as part of the schema of 'fv'.
                    # Example: FeatureService(features=[fv]), where 'fv' was defined with a schema.
                    continue

                # The projection wants to select all possible inferred features.
                # Example: FeatureService(features=[fv]), where 'fv' is a feature view that
                # was defined without a schema.
                if feature_grouping.name in fvs_to_update:
                    projection.features = fvs_to_update[feature_grouping.name].features
                else:
                    raise FeatureViewMissingDuringFeatureServiceInference(
                        feature_view_name=feature_grouping.name,
                        feature_service_name=self.name,
                    )
            else:
                raise ValueError(
                    f"The feature service {self.name} has been provided with an invalid type "
                    f'{type(feature_grouping)} as part of the "features" argument.)'
                )

    def prepare_for_apply(
        self,
        registry: "BaseRegistry",
        project: str,
        allow_cache: bool = False,
    ) -> "FeatureService":
        """
        Materialize feature view projections before registry apply.

        Uses the same FeatureService construction and ``infer_features`` path as
        ``FeatureStore.apply`` for SDK-defined services.

        When the service is already fully resolved (SDK path where _features is
        set and projections already have features populated via infer_features,
        OR the proto deserialization path where projections carry full dtype
        info), this is a no-op.
        """
        from feast.types import Invalid

        if self._features and all(p.features for p in self.feature_view_projections):
            return self

        if (
            not self._features
            and self.feature_view_projections
            and all(
                p.features and all(f.dtype != Invalid for f in p.features)
                for p in self.feature_view_projections
            )
        ):
            return self

        fvs_to_update: Dict[str, Union[FeatureView, BaseFeatureView]] = {}

        if self._features:
            for feature_grouping in self._features:
                if isinstance(feature_grouping, BaseFeatureView):
                    fvs_to_update[feature_grouping.name] = (
                        registry.get_any_feature_view(
                            feature_grouping.name, project, allow_cache=allow_cache
                        )
                    )
            self.infer_features(fvs_to_update=fvs_to_update)
            return self

        resolved_features: List[Union[FeatureView, OnDemandFeatureView, LabelView]] = []
        for projection in self.feature_view_projections:
            try:
                feature_view = registry.get_any_feature_view(
                    projection.name, project, allow_cache=allow_cache
                )
            except FeastObjectNotFoundException as exc:
                raise FeastObjectNotFoundException(
                    f"Feature view '{projection.name}' not found in project '{project}'"
                ) from exc

            if not isinstance(
                feature_view, (FeatureView, OnDemandFeatureView, LabelView)
            ):
                raise ValueError(
                    f"Cannot resolve projection for feature view '{projection.name}'"
                )

            fvs_to_update[feature_view.name] = feature_view
            features_by_name = {
                feature.name: feature for feature in feature_view.features
            }

            if self._projection_matches_registry_features(projection, features_by_name):
                resolved_features.append(feature_view.with_projection(projection))
            elif projection.desired_features:
                resolved_features.append(
                    feature_view[list(projection.desired_features)]
                )
            elif not projection.features:
                resolved_features.append(feature_view)
            else:
                resolved_features.append(
                    feature_view[[feature.name for feature in projection.features]]
                )

        prepared = FeatureService(
            name=self.name,
            features=resolved_features,
            tags=self.tags,
            description=self.description,
            owner=self.owner,
            logging_config=self.logging_config,
            precompute_online=self.precompute_online,
        )
        prepared.created_timestamp = self.created_timestamp
        prepared.last_updated_timestamp = self.last_updated_timestamp
        prepared.infer_features(fvs_to_update=fvs_to_update)

        self._features = prepared._features
        self.feature_view_projections = prepared.feature_view_projections
        return self

    @staticmethod
    def _projection_matches_registry_features(
        projection: FeatureViewProjection,
        features_by_name: Dict[str, Field],
    ) -> bool:
        if not projection.features:
            return False

        for feature in projection.features:
            if feature.name not in features_by_name:
                return False
            if feature != features_by_name[feature.name]:
                return False
        return True

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
            or self.precompute_online != other.precompute_online
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
            precompute_online=feature_service_proto.spec.precompute_online,
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
            precompute_online=self.precompute_online,
        )

        return FeatureServiceProto(spec=spec, meta=meta)

    @classmethod
    def build_apply_request(
        cls,
        *,
        name: str,
        project: str,
        feature_view_refs: List[tuple[str, Optional[List[str]]]],
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        commit: bool = True,
    ):
        """Build an unresolved ApplyFeatureServiceRequest from feature view refs."""
        from feast.protos.feast.core.Feature_pb2 import FeatureSpecV2
        from feast.protos.feast.core.FeatureViewProjection_pb2 import (
            FeatureViewProjection as FeatureViewProjectionProto,
        )
        from feast.protos.feast.registry import RegistryServer_pb2

        projections = []
        for feature_view_name, feature_names in feature_view_refs:
            projection = FeatureViewProjectionProto(
                feature_view_name=feature_view_name,
            )
            if feature_names:
                for feature_name in feature_names:
                    projection.feature_columns.append(FeatureSpecV2(name=feature_name))
            projections.append(projection)

        spec = FeatureServiceSpecProto(
            name=name,
            features=projections,
            tags=tags or {},
            description=description,
            owner=owner,
        )
        return RegistryServer_pb2.ApplyFeatureServiceRequest(
            feature_service=FeatureServiceProto(spec=spec),
            project=project,
            commit=commit,
        )

    def validate(self):
        if not self.precompute_online:
            return

        for fv in self._features:
            if isinstance(fv, OnDemandFeatureView) and not fv.write_to_online_store:
                raise ValueError(
                    f"FeatureService '{self.name}' has precompute_online=True but "
                    f"contains OnDemandFeatureView '{fv.name}' with "
                    f"write_to_online_store=False. On-demand transforms computed at "
                    f"serve time cannot be pre-computed."
                )
