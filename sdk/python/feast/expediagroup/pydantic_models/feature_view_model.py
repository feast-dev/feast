"""
Pydantic Model for Data Source

Copyright 2023 Expedia Group
Author: matcarlin@expediagroup.com
"""
import sys
from datetime import timedelta
from json import dumps
from typing import Callable, Dict, List, Optional

from pydantic import BaseModel

from feast.data_source import DataSource
from feast.entity import Entity
from feast.expediagroup.pydantic_models.data_source_model import (
    AnyDataSource,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.feature_view import FeatureView
from feast.field import Field
from feast.types import ComplexFeastType, PrimitiveFeastType

SUPPORTED_DATA_SOURCES = [RequestSourceModel, SparkSourceModel]


class FeatureViewModel(BaseModel):
    """
    Pydantic Model of a Feast FeatureView.
    """

    name: str
    original_entities: List[EntityModel] = []
    original_schema: Optional[List[Field]] = None
    ttl: Optional[timedelta]
    batch_source: AnyDataSource
    stream_source: Optional[AnyDataSource]
    online: bool = True
    description: str = ""
    tags: Optional[Dict[str, str]] = None
    owner: str = ""

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
        json_encoders: Dict[object, Callable] = {
            Field: lambda v: int(dumps(v.value, default=str)),
            DataSource: lambda v: v.to_pydantic_model(),
            Entity: lambda v: v.to_pydantic_model(),
            ComplexFeastType: lambda v: str(v),
            PrimitiveFeastType: lambda v: str(v),
        }

    def to_feature_view(self):
        """
        Given a Pydantic FeatureViewModel, create and return a FeatureView.

        Returns:
            A FeatureView.
        """
        # Convert each of the sources if they exist
        batch_source = self.batch_source.to_data_source() if self.batch_source else None
        stream_source = (
            self.stream_source.to_data_source() if self.stream_source else None
        )

        # Mirror the stream/batch source conditions in the FeatureView
        # constructor; one source is passed, either a stream source
        # which contains a batch source inside it, or a batch source
        # on its own.
        source = stream_source if stream_source else batch_source
        if stream_source:
            source.batch_source = batch_source

        # Create the FeatureView
        feature_view = FeatureView(
            name=self.name,
            source=source,
            schema=self.original_schema,
            entities=[entity.to_entity() for entity in self.original_entities],
            ttl=self.ttl,
            online=self.online,
            description=self.description,
            tags=self.tags if self.tags else None,
            owner=self.owner,
        )

        return feature_view

    @classmethod
    def from_feature_view(cls, feature_view):
        """
        Converts a FeatureView object to its pydantic model representation.

        Returns:
            A FeatureViewModel.
        """
        batch_source = None
        if feature_view.batch_source:
            class_ = getattr(
                sys.modules[__name__],
                type(feature_view.batch_source).__name__ + "Model",
            )
            if class_ not in SUPPORTED_DATA_SOURCES:
                raise ValueError(
                    "Batch source type is not a supported data source type."
                )
            batch_source = class_.from_data_source(feature_view.batch_source)
        stream_source = None
        if feature_view.stream_source:
            class_ = getattr(
                sys.modules[__name__],
                type(feature_view.stream_source).__name__ + "Model",
            )
            if class_ not in SUPPORTED_DATA_SOURCES:
                raise ValueError(
                    "Stream source type is not a supported data source type."
                )
            stream_source = class_.from_data_source(feature_view.stream_source)
        return cls(
            name=feature_view.name,
            original_entities=[
                EntityModel.from_entity(entity)
                for entity in feature_view.original_entities
            ],
            ttl=feature_view.ttl,
            original_schema=feature_view.original_schema,
            batch_source=batch_source,
            stream_source=stream_source,
            online=feature_view.online,
            description=feature_view.description,
            tags=feature_view.tags if feature_view.tags else None,
            owner=feature_view.owner,
        )
