"""
Pydantic Model for Data Source

Copyright 2023 Expedia Group
Author: matcarlin@expediagroup.com
"""

import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

import dill
from pydantic import BaseModel, field_serializer, field_validator
from typing_extensions import Self

from feast.expediagroup.pydantic_models.data_source_model import (
    AnyBatchDataSource,
    KafkaSourceModel,
    RequestSourceModel,
    SparkSourceModel,
)
from feast.expediagroup.pydantic_models.entity_model import EntityModel
from feast.expediagroup.pydantic_models.field_model import FieldModel
from feast.feature_view import FeatureView
from feast.feature_view_projection import FeatureViewProjection
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.transformation.pandas_transformation import PandasTransformation
from feast.transformation.python_transformation import PythonTransformation
from feast.transformation.substrait_transformation import SubstraitTransformation

# TO DO: Supported batch and supported streaming
SUPPORTED_BATCH_DATA_SOURCES = [RequestSourceModel, SparkSourceModel]
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
    batch_source: Union[AnyBatchDataSource]
    stream_source: Optional[KafkaSourceModel]
    online: bool
    description: str
    tags: Optional[Dict[str, str]]
    owner: str
    materialization_intervals: List[Tuple[datetime, datetime]] = []
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]

    # To make it compatible with Pydantic V1, we need this field_serializer
    @field_serializer("ttl")
    def serialize_ttl(self, ttl: timedelta):
        return timedelta.total_seconds(ttl) if ttl else None

    # To make it compatible with Pydantic V1, we need this field_validator
    @field_validator("ttl", mode="before")
    @classmethod
    def validate_ttl(cls, v: Optional[Union[int, float, str, timedelta]]):
        try:
            if isinstance(v, timedelta):
                return v
            elif isinstance(v, float):
                return timedelta(seconds=v)
            elif isinstance(v, str):
                return timedelta(seconds=float(v))
            elif isinstance(v, int):
                return timedelta(seconds=v)
            else:
                return timedelta(seconds=0)  # Default value
        except ValueError:
            raise ValueError("ttl must be one of the int, float, str, timedelta types")

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
            source.batch_source = batch_source  # type: ignore

        # Create the FeatureView
        feature_view = FeatureView(
            name=self.name,
            source=source,  # type: ignore
            schema=(
                [sch.to_field() for sch in self.original_schema]
                if self.original_schema is not None
                else None
            ),
            entities=[entity.to_entity() for entity in self.original_entities],
            ttl=self.ttl,
            online=self.online,
            description=self.description,
            tags=self.tags if self.tags else None,
            owner=self.owner,
        )
        feature_view.materialization_intervals = self.materialization_intervals
        feature_view.created_timestamp = self.created_timestamp
        feature_view.last_updated_timestamp = self.last_updated_timestamp

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
            original_schema=(
                [
                    FieldModel.from_field(fv_schema)
                    for fv_schema in feature_view.original_schema
                ]
                if feature_view.original_schema is not None
                else None
            ),
            batch_source=batch_source,  # type: ignore
            stream_source=stream_source,
            online=feature_view.online,
            description=feature_view.description,
            tags=feature_view.tags if feature_view.tags else None,
            owner=feature_view.owner,
            materialization_intervals=feature_view.materialization_intervals,
            created_timestamp=feature_view.created_timestamp,
            last_updated_timestamp=feature_view.last_updated_timestamp,
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


class PandasTransformationModel(BaseModel):
    """
    Pydantic Model of a Feast PandasTransformation.
    """

    udf: str
    udf_string: str

    def to_pandas_transformation(self) -> PandasTransformation:
        return PandasTransformation(
            udf=dill.loads(bytes.fromhex(self.udf)), udf_string=self.udf_string
        )

    @classmethod
    def from_pandas_transformation(
        cls,
        pandas_transformation: PandasTransformation,
    ) -> Self:
        return cls(
            udf=dill.dumps(pandas_transformation.udf, recurse=True).hex(),
            udf_string=pandas_transformation.udf_string,
        )


class PythonTransformationModel(BaseModel):
    """
    Pydantic Model of a Feast PythonTransformation.
    """

    udf: str
    udf_string: str

    def to_python_transformation(self) -> PythonTransformation:
        return PythonTransformation(
            udf=dill.loads(bytes.fromhex(self.udf)), udf_string=self.udf_string
        )

    @classmethod
    def from_python_transformation(
        cls,
        python_transformation: PythonTransformation,
    ) -> Self:
        return cls(
            udf=dill.dumps(python_transformation.udf, recurse=True).hex(),
            udf_string=python_transformation.udf_string,
        )


class SubstraitTransformationModel(BaseModel):
    """
    Pydantic Model of a Feast SubstraitTransformation.
    """

    substrait_plan: bytes
    ibis_function: str

    def to_substrait_transformation(self) -> SubstraitTransformation:
        return SubstraitTransformation(
            substrait_plan=self.substrait_plan,
            ibis_function=dill.loads(bytes.fromhex(self.ibis_function)),
        )

    @classmethod
    def from_substrait_transformation(
        cls,
        substrait_transformation: SubstraitTransformation,
    ) -> Self:
        return cls(
            substrait_plan=substrait_transformation.substrait_plan,
            ibis_function=dill.dumps(
                substrait_transformation.ibis_function, recurse=True
            ).hex(),
        )


class OnDemandFeatureViewModel(BaseFeatureViewModel):
    """
    Pydantic Model of a Feast OnDemandFeatureView.
    """

    name: str
    features: List[FieldModel]
    source_feature_view_projections: Dict[str, FeatureViewProjectionModel]
    source_request_sources: Dict[str, RequestSourceModel]
    udf: str = ""
    udf_string: str = ""
    feature_transformation: Optional[
        Union[
            PandasTransformationModel,
            PythonTransformationModel,
            SubstraitTransformationModel,
        ]
    ] = None
    mode: str = "pandas"
    description: str = ""
    tags: Optional[dict[str, str]] = None
    owner: str = ""
    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    def to_feature_view(self) -> OnDemandFeatureView:
        source_request_sources = dict()
        if self.source_request_sources:
            for key, feature_view_projection in self.source_request_sources.items():
                source_request_sources[key] = feature_view_projection.to_data_source()

        source_feature_view_projections = dict()
        if self.source_feature_view_projections:
            for (  # type: ignore
                key,
                feature_view_projection,
            ) in self.source_feature_view_projections.items():
                source_feature_view_projections[key] = (
                    feature_view_projection.to_feature_view_projection()  # type: ignore
                )

        if self.feature_transformation is not None:
            if self.mode == "pandas":
                feature_transformation = (
                    self.feature_transformation.to_pandas_transformation()  # type: ignore
                )
            elif self.mode == "python":
                feature_transformation = (
                    self.feature_transformation.to_python_transformation()  # type: ignore
                )
            elif self.mode == "substrait":
                feature_transformation = (
                    self.feature_transformation.to_substrait_transformation()  # type: ignore
                )
        else:
            feature_transformation = PandasTransformation(
                udf=dill.loads(bytes.fromhex(self.udf)), udf_string=self.udf_string
            )

        odfv = OnDemandFeatureView(
            name=self.name,
            schema=[sch.to_field() for sch in self.features],
            sources=list(source_feature_view_projections.values())
            + list(source_request_sources.values()),
            udf=dill.loads(bytes.fromhex(self.udf)),
            udf_string=self.udf_string,
            feature_transformation=feature_transformation,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        odfv.created_timestamp = self.created_timestamp
        odfv.last_updated_timestamp = self.last_updated_timestamp
        return odfv

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
                source_feature_view_projections[key] = (
                    FeatureViewProjectionModel.from_feature_view_projection(
                        feature_view_projection
                    )
                )
        udf = ""
        udf_string = ""
        if on_demand_feature_view.mode == "pandas":
            feature_transformation = (
                PandasTransformationModel.from_pandas_transformation(
                    on_demand_feature_view.feature_transformation  # type: ignore
                )
            )
            udf = dill.dumps(
                on_demand_feature_view.feature_transformation.udf,  # type: ignore
                recurse=True,
            ).hex()
            udf_string = on_demand_feature_view.feature_transformation.udf_string  # type: ignore
        elif on_demand_feature_view.mode == "python":
            feature_transformation = (
                PythonTransformationModel.from_python_transformation(
                    on_demand_feature_view.feature_transformation  # type: ignore
                )
            )
            udf = dill.dumps(
                on_demand_feature_view.feature_transformation.udf,  # type: ignore
                recurse=True,
            ).hex()
            udf_string = on_demand_feature_view.feature_transformation.udf_string  # type: ignore
        elif on_demand_feature_view.mode == "substrait":
            feature_transformation = (
                SubstraitTransformationModel.from_substrait_transformation(
                    on_demand_feature_view.feature_transformation  # type: ignore
                )
            )

        return cls(
            name=on_demand_feature_view.name,
            features=[
                FieldModel.from_field(feature)
                for feature in on_demand_feature_view.features
            ],
            source_feature_view_projections=source_feature_view_projections,
            source_request_sources=source_request_sources,
            udf=udf,
            udf_string=udf_string,
            feature_transformation=feature_transformation,
            description=on_demand_feature_view.description,
            tags=on_demand_feature_view.tags,
            owner=on_demand_feature_view.owner,
            created_timestamp=on_demand_feature_view.created_timestamp,
            last_updated_timestamp=on_demand_feature_view.last_updated_timestamp,
        )
