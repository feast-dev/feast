import sys
from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel
from typing_extensions import Self

from feast.expediagroup.pydantic_models.feature_view_model import (
    FeatureViewModel,
    FeatureViewProjectionModel,
    OnDemandFeatureViewModel,
)
from feast.feature_service import FeatureService


class FeatureServiceModel(BaseModel):
    """
    Pydantic model for Feast FeatureService
    """

    name: str
    features: List[Union[FeatureViewModel, OnDemandFeatureViewModel]]
    feature_view_projections: List[FeatureViewProjectionModel]
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]
    # TODO: logging_config option is not supported temporarily.
    # we will add this fucntionality to FeatureServiceModel in future.
    # logging_config: Optional[LoggingConfig] = None

    def to_feature_service(self) -> FeatureService:
        fs = FeatureService(
            name=self.name,
            features=[feature.to_feature_view() for feature in self.features],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        fs.feature_view_projections = [
            feature_view_projection.to_feature_view_projection()
            for feature_view_projection in self.feature_view_projections
        ]
        fs.created_timestamp = self.created_timestamp
        fs.last_updated_timestamp = self.last_updated_timestamp

        return fs

    @classmethod
    def from_feature_service(
        cls,
        feature_service: FeatureService,
    ) -> Self:  # type: ignore
        features = []
        for feature in feature_service._features:
            class_ = getattr(
                sys.modules[__name__],
                type(feature).__name__ + "Model",
            )
            features.append(class_.from_feature_view(feature))

        feature_view_projections = [
            FeatureViewProjectionModel.from_feature_view_projection(
                feature_view_projection
            )
            for feature_view_projection in feature_service.feature_view_projections
        ]

        return cls(
            name=feature_service.name,
            features=features,
            feature_view_projections=feature_view_projections,
            description=feature_service.description,
            tags=feature_service.tags,
            owner=feature_service.owner,
            created_timestamp=feature_service.created_timestamp,
            last_updated_timestamp=feature_service.last_updated_timestamp,
        )
