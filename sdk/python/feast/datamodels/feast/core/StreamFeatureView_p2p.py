# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing
from datetime import timedelta

from protobuf_to_pydantic.util import Timedelta
from pydantic import BaseModel, BeforeValidator, Field
from typing_extensions import Annotated

from .Aggregation_p2p import Aggregation
from .DataSource_p2p import DataSource
from .Feature_p2p import FeatureSpecV2
from .FeatureView_p2p import FeatureViewMeta
from .OnDemandFeatureView_p2p import UserDefinedFunction
from .Transformation_p2p import FeatureTransformationV2


class StreamFeatureViewSpec(BaseModel):
    """
    Next available id: 17
    """

    # Name of the feature view. Must be unique. Not updated.
    name: str = Field(default="")
    # Name of Feast project that this feature view belongs to.
    project: str = Field(default="")
    # List of names of entities associated with this feature view.
    entities: typing.List[str] = Field(default_factory=list)
    # List of specifications for each feature defined as part of this feature view.
    features: typing.List[FeatureSpecV2] = Field(default_factory=list)
    # List of specifications for each entity defined as part of this feature view.
    entity_columns: typing.List[FeatureSpecV2] = Field(default_factory=list)
    # Description of the feature view.
    description: str = Field(default="")
    # User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    # Owner of the feature view.
    owner: str = Field(default="")
    # Features in this feature view can only be retrieved from online serving
    # younger than ttl. Ttl is measured as the duration of time between
    # the feature's event timestamp and when the feature is retrieved
    # Feature values outside ttl will be returned as unset values and indicated to end user
    ttl: Annotated[timedelta, BeforeValidator(Timedelta.validate)] = Field(
        default_factory=timedelta
    )
    # Batch/Offline DataSource where this view can retrieve offline feature data.
    batch_source: typing.Optional[DataSource] = Field(default=None)
    # Streaming DataSource from where this view can consume "online" feature data.
    stream_source: typing.Optional[DataSource] = Field(default=None)
    # Whether these features should be served online or not
    online: bool = Field(default=False)
    # Serialized function that is encoded in the streamfeatureview
    user_defined_function: UserDefinedFunction = Field(
        default_factory=UserDefinedFunction
    )
    # Mode of execution
    mode: str = Field(default="")
    # Aggregation definitions
    aggregations: typing.List[Aggregation] = Field(default_factory=list)
    # Timestamp field for aggregation
    timestamp_field: str = Field(default="")
    # Oneof with {user_defined_function, on_demand_substrait_transformation}
    feature_transformation: FeatureTransformationV2 = Field(
        default_factory=FeatureTransformationV2
    )


class StreamFeatureView(BaseModel):
    # User-specified specifications of this feature view.
    spec: StreamFeatureViewSpec = Field(default_factory=StreamFeatureViewSpec)
    meta: FeatureViewMeta = Field(default_factory=FeatureViewMeta)
