"""
Pydantic Model for Data Source

Copyright 2023 Expedia Group
Author: matcarlin@expediagroup.com
"""
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import dill
from pydantic import BaseModel
from typing_extensions import Self

from feast.expediagroup.pydantic_models.data_source_model import (
    AnyBatchDataSource,
    KafkaSourceModel,
    PushSourceModel,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.expediagroup.pydantic_models.field_model import FieldModel
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.on_demand_feature_view import OnDemandFeatureView

# TO DO: Supported batch and supported streaming
SUPPORTED_BATCH_DATA_SOURCES = [RequestSourceModel, SparkSourceModel, PushSourceModel]
SUPPORTED_STREAM_DATA_SOURCES = [KafkaSourceModel]


class BaseFeatureViewModel(BaseModel):
    """
    Pydantic Model of a Feast BaseFeatureView.
    """

    def to_feature_view(self):
        """
        Given a Pydantic BaseFeatureViewModel, create and return a FeatureView.

        Returns:
            A FeatureView.
        """
        raise NotImplementedError

    @classmethod
    def from_feature_view(cls, feature_view):
        """
        Converts a FeatureView object to its pydantic model representation.

        Returns:
            A BaseFeatureViewModel.
        """
        raise NotImplementedError


class FeatureViewModel(BaseFeatureViewModel):
    """
    Pydantic Model of a Feast FeatureView.
    """

    name: str
    original_entities: List[EntityModel] = []
    original_schema: Optional[List[FieldModel]]
    ttl: Optional[timedelta]
    batch_source: AnyBatchDataSource
    stream_source: Optional[KafkaSourceModel]
    online: bool
    description: str
    tags: Optional[Dict[str, str]]
    owner: str
    materialization_intervals: List[Tuple[datetime, datetime]] = []

    def to_feature_view(self) -> FeatureView:
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
            schema=[sch.to_field() for sch in self.original_schema]
            if self.original_schema is not None
            else None,
            entities=[entity.to_entity() for entity in self.original_entities],
            ttl=self.ttl,
            online=self.online,
            description=self.description,
            tags=self.tags if self.tags else None,
            owner=self.owner,
        )
        feature_view.materialization_intervals = self.materialization_intervals

        return feature_view

    @classmethod
    def from_feature_view(
        cls,
        feature_view: FeatureView,
    ) -> Self:  # type: ignore
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
            if class_ not in SUPPORTED_BATCH_DATA_SOURCES:
                raise ValueError(
                    "Batch source type is not a supported data source type."
                )
            batch_source = class_.from_data_source(feature_view.batch_source)
        # For the time being, Pydantic models only support KafkaSource for streaming source,
        # so it is no longer necessary to dynamically create the stream_source model based
        # on a parameter.
        stream_source = None
        if feature_view.stream_source:
            stream_source = KafkaSourceModel.from_data_source(
                feature_view.stream_source
            )
        return cls(
            name=feature_view.name,
            original_entities=[
                EntityModel.from_entity(entity)
                for entity in feature_view.original_entities
            ],
            ttl=feature_view.ttl,
            original_schema=[
                FieldModel.from_field(fv_schema)
                for fv_schema in feature_view.original_schema
            ]
            if feature_view.original_schema is not None
            else None,
            batch_source=batch_source,
            stream_source=stream_source,
            online=feature_view.online,
            description=feature_view.description,
            tags=feature_view.tags if feature_view.tags else None,
            owner=feature_view.owner,
            materialization_intervals=feature_view.materialization_intervals,
        )


class FeatureViewProjectionModel(BaseModel):
    """
    Pydantic Model of a Feast FeatureViewProjection.
    """

    name: str
    name_alias: Optional[str]
    features: List[FieldModel]
    desired_features: List[str]
    join_key_map: Dict[str, str]

    def to_feature_view_projection(self) -> FeatureViewProjection:
        return FeatureViewProjection(
            name=self.name,
            name_alias=self.name_alias,
            desired_features=self.desired_features,
            features=[sch.to_field() for sch in self.features],
            join_key_map=self.join_key_map,
        )

    @classmethod
    def from_feature_view_projection(
        cls,
        feature_view_projection: FeatureViewProjection,
    ) -> Self:  # type: ignore
        return cls(
            name=feature_view_projection.name,
            name_alias=feature_view_projection.name_alias,
            desired_features=feature_view_projection.desired_features,
            features=[
                FieldModel.from_field(feature)
                for feature in feature_view_projection.features
            ],
            join_key_map=feature_view_projection.join_key_map,
        )


class OnDemandFeatureViewModel(BaseFeatureViewModel):
    """
    Pydantic Model of a Feast OnDemandFeatureView.
    """

    name: str
    features: List[FieldModel]
    source_feature_view_projections: Dict[str, FeatureViewProjectionModel]
    source_request_sources: Dict[str, RequestSourceModel]
    udf: str
    udf_string: str
    description: str
    tags: Dict[str, str]
    owner: str

    def to_feature_view(self) -> OnDemandFeatureView:
        source_request_sources = dict()
        if self.source_request_sources:
            for key, feature_view_projection in self.source_request_sources.items():
                source_request_sources[key] = feature_view_projection.to_data_source()

        source_feature_view_projections = dict()
        if self.source_feature_view_projections:
            for (
                key,
                feature_view_projection,
            ) in self.source_feature_view_projections.items():
                source_feature_view_projections[
                    key
                ] = feature_view_projection.to_feature_view_projection()

        return OnDemandFeatureView(
            name=self.name,
            schema=[sch.to_field() for sch in self.features],
            sources=list(source_feature_view_projections.values())
            + list(source_request_sources.values()),
            udf=dill.loads(bytes.fromhex(self.udf)),
            udf_string=self.udf_string,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

    @classmethod
    def from_feature_view(
        cls,
        on_demand_feature_view: OnDemandFeatureView,
    ) -> Self:  # type: ignore
        source_request_sources = dict()
        if on_demand_feature_view.source_request_sources:
            for (
                key,
                req_data_source,
            ) in on_demand_feature_view.source_request_sources.items():
                source_request_sources[key] = RequestSourceModel.from_data_source(
                    req_data_source
                )

        source_feature_view_projections = dict()
        if on_demand_feature_view.source_feature_view_projections:
            for (
                key,
                feature_view_projection,
            ) in on_demand_feature_view.source_feature_view_projections.items():
                source_feature_view_projections[
                    key
                ] = FeatureViewProjectionModel.from_feature_view_projection(
                    feature_view_projection
                )

        return cls(
            name=on_demand_feature_view.name,
            features=[
                FieldModel.from_field(feature)
                for feature in on_demand_feature_view.features
            ],
            source_feature_view_projections=source_feature_view_projections,
            source_request_sources=source_request_sources,
            udf=dill.dumps(on_demand_feature_view.udf, recurse=True).hex(),
            udf_string=on_demand_feature_view.udf_string,
            description=on_demand_feature_view.description,
            tags=on_demand_feature_view.tags,
            owner=on_demand_feature_view.owner,
        )
