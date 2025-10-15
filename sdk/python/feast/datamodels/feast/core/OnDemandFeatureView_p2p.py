# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
# PATCHED by patch_datamodels.py v4 - OnDemandFeatureView_p2p.py
import typing
from datetime import datetime

from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, Field, model_validator

from .DataSource_p2p import DataSource
from .Feature_p2p import FeatureSpecV2
from .FeatureView_p2p import FeatureView
from .FeatureViewProjection_p2p import FeatureViewProjection
from .Transformation_p2p import FeatureTransformationV2


class UserDefinedFunction(BaseModel):
    """
    Serialized representation of python function.
    """

    # The function name
    name: str = Field(default="")
    # The python-syntax function body (serialized by dill)
    body: bytes = Field(default=b"")
    # The string representation of the udf
    body_text: str = Field(default="")


class OnDemandFeatureViewSpec(BaseModel):
    """
    Next available id: 9
    """

    # Name of the feature view. Must be unique. Not updated.
    name: str = Field(default="")
    # Name of Feast project that this feature view belongs to.
    project: str = Field(default="")
    # List of features specifications for each feature defined with this feature view.
    features: typing.List[FeatureSpecV2] = Field(default_factory=list)
    # Map of sources for this feature view.
    sources: "typing.Dict[str, OnDemandSource]" = Field(default_factory=dict)
    user_defined_function: UserDefinedFunction = Field(
        default_factory=UserDefinedFunction
    )
    # Oneof with {user_defined_function, on_demand_substrait_transformation}
    feature_transformation: FeatureTransformationV2 = Field(
        default_factory=FeatureTransformationV2
    )
    # Description of the on demand feature view.
    description: str = Field(default="")
    # User defined metadata.
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    # Owner of the on demand feature view.
    owner: str = Field(default="")
    mode: str = Field(default="")
    write_to_online_store: bool = Field(default=False)
    # List of names of entities associated with this feature view.
    entities: typing.List[str] = Field(default_factory=list)
    # List of specifications for each entity defined as part of this feature view.
    entity_columns: typing.List[FeatureSpecV2] = Field(default_factory=list)
    singleton: bool = Field(default=False)


class OnDemandFeatureViewMeta(BaseModel):
    # Time where this Feature View is created
    created_timestamp: datetime = Field(default_factory=datetime.now)
    # Time where this Feature View is last updated
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)


class OnDemandFeatureView(BaseModel):
    # User-specified specifications of this feature view.
    spec: OnDemandFeatureViewSpec = Field(default_factory=OnDemandFeatureViewSpec)
    meta: OnDemandFeatureViewMeta = Field(default_factory=OnDemandFeatureViewMeta)


class OnDemandSource(BaseModel):
    _one_of_dict = {
        "OnDemandSource.source": {
            "fields": {"feature_view", "feature_view_projection", "request_data_source"}
        }
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    feature_view: FeatureView = Field(default_factory=FeatureView)
    feature_view_projection: FeatureViewProjection = Field(
        default_factory=FeatureViewProjection
    )
    request_data_source: DataSource = Field(default_factory=DataSource)


class OnDemandFeatureViewList(BaseModel):
    ondemandfeatureviews: typing.List[OnDemandFeatureView] = Field(default_factory=list)
