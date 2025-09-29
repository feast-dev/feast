# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing
from datetime import datetime

from pydantic import BaseModel, Field

from .DataSource_p2p import DataSource
from .Entity_p2p import Entity
from .FeatureService_p2p import FeatureService
from .FeatureTable_p2p import FeatureTable
from .FeatureView_p2p import FeatureView
from .InfraObject_p2p import Infra
from .OnDemandFeatureView_p2p import OnDemandFeatureView
from .Permission_p2p import Permission
from .Project_p2p import Project
from .SavedDataset_p2p import SavedDataset
from .StreamFeatureView_p2p import StreamFeatureView
from .ValidationProfile_p2p import ValidationReference


class ProjectMetadata(BaseModel):
    project: str = Field(default="")
    project_uuid: str = Field(default="")


class Registry(BaseModel):
    """
    Next id: 18
    """

    entities: typing.List[Entity] = Field(default_factory=list)
    feature_tables: typing.List[FeatureTable] = Field(default_factory=list)
    feature_views: typing.List[FeatureView] = Field(default_factory=list)
    data_sources: typing.List[DataSource] = Field(default_factory=list)
    on_demand_feature_views: typing.List[OnDemandFeatureView] = Field(
        default_factory=list
    )
    stream_feature_views: typing.List[StreamFeatureView] = Field(default_factory=list)
    feature_services: typing.List[FeatureService] = Field(default_factory=list)
    saved_datasets: typing.List[SavedDataset] = Field(default_factory=list)
    validation_references: typing.List[ValidationReference] = Field(
        default_factory=list
    )
    infra: Infra = Field(default_factory=Infra)
    # Tracking metadata of Feast by project
    project_metadata: typing.List[ProjectMetadata] = Field(default_factory=list)
    registry_schema_version: str = Field(
        default=""
    )  # to support migrations; incremented when schema is changed
    version_id: str = Field(
        default=""
    )  # version id, random string generated on each update of the data; now used only for debugging purposes
    last_updated: datetime = Field(default_factory=datetime.now)
    permissions: typing.List[Permission] = Field(default_factory=list)
    projects: typing.List[Project] = Field(default_factory=list)
