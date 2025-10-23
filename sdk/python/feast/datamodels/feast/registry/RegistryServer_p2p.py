# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
# PATCHED by patch_datamodels.py v4 - RegistryServer_p2p.py
import typing
from datetime import datetime

from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, Field, model_validator

from ..core.DataSource_p2p import DataSource
from ..core.Entity_p2p import Entity
from ..core.FeatureService_p2p import FeatureService
from ..core.FeatureView_p2p import FeatureView
from ..core.InfraObject_p2p import Infra
from ..core.OnDemandFeatureView_p2p import OnDemandFeatureView
from ..core.Permission_p2p import Permission
from ..core.Project_p2p import Project
from ..core.Registry_p2p import ProjectMetadata
from ..core.SavedDataset_p2p import SavedDataset
from ..core.StreamFeatureView_p2p import StreamFeatureView
from ..core.ValidationProfile_p2p import ValidationReference


class PaginationParams(BaseModel):
    """
    Common pagination and sorting messages
    """

    page: int = Field(default=0)  # 1-based page number
    limit: int = Field(default=0)  # Number of items per page


class SortingParams(BaseModel):
    sort_by: str = Field(default="")  # Field to sort by (supports dot notation)
    sort_order: str = Field(default="")  # "asc" or "desc"


class PaginationMetadata(BaseModel):
    page: int = Field(default=0)
    limit: int = Field(default=0)
    total_count: int = Field(default=0)
    total_pages: int = Field(default=0)
    has_next: bool = Field(default=False)
    has_previous: bool = Field(default=False)


class RefreshRequest(BaseModel):
    project: str = Field(default="")


class UpdateInfraRequest(BaseModel):
    infra: Infra = Field(default_factory=Infra)
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetInfraRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListProjectMetadataRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListProjectMetadataResponse(BaseModel):
    project_metadata: typing.List[ProjectMetadata] = Field(default_factory=list)


class ApplyMaterializationRequest(BaseModel):
    feature_view: FeatureView = Field(default_factory=FeatureView)
    project: str = Field(default="")
    start_date: datetime = Field(default_factory=datetime.now)
    end_date: datetime = Field(default_factory=datetime.now)
    commit: bool = Field(default=False)


class ApplyEntityRequest(BaseModel):
    entity: Entity = Field(default_factory=Entity)
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetEntityRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListEntitiesRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListEntitiesResponse(BaseModel):
    entities: typing.List[Entity] = Field(default_factory=list)
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeleteEntityRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    commit: bool = Field(default=False)


class ApplyDataSourceRequest(BaseModel):
    data_source: DataSource = Field(default_factory=DataSource)
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetDataSourceRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListDataSourcesRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListDataSourcesResponse(BaseModel):
    data_sources: typing.List[DataSource] = Field(
        default_factory=list, alias="dataSources"
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeleteDataSourceRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    commit: bool = Field(default=False)


class ApplyFeatureViewRequest(BaseModel):
    _one_of_dict = {
        "ApplyFeatureViewRequest.base_feature_view": {
            "fields": {"feature_view", "on_demand_feature_view", "stream_feature_view"}
        }
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    feature_view: FeatureView = Field(default_factory=FeatureView)
    on_demand_feature_view: OnDemandFeatureView = Field(
        default_factory=OnDemandFeatureView
    )
    stream_feature_view: StreamFeatureView = Field(default_factory=StreamFeatureView)
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetFeatureViewRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListFeatureViewsRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListFeatureViewsResponse(BaseModel):
    feature_views: typing.List[FeatureView] = Field(
        default_factory=list, alias="featureViews"
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeleteFeatureViewRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    commit: bool = Field(default=False)


class AnyFeatureView(BaseModel):
    _one_of_dict = {
        "AnyFeatureView.any_feature_view": {
            "fields": {"feature_view", "on_demand_feature_view", "stream_feature_view"}
        }
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    feature_view: FeatureView = Field(default_factory=FeatureView)
    on_demand_feature_view: OnDemandFeatureView = Field(
        default_factory=OnDemandFeatureView
    )
    stream_feature_view: StreamFeatureView = Field(default_factory=StreamFeatureView)


class GetAnyFeatureViewRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class GetAnyFeatureViewResponse(BaseModel):
    any_feature_view: AnyFeatureView = Field(default_factory=AnyFeatureView)


class ListAllFeatureViewsRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    entity: str = Field(default="")
    feature: str = Field(default="")
    feature_service: str = Field(default="")
    data_source: str = Field(default="")
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListAllFeatureViewsResponse(BaseModel):
    feature_views: typing.List[AnyFeatureView] = Field(
        default_factory=list, alias="featureViews"
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class GetStreamFeatureViewRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListStreamFeatureViewsRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListStreamFeatureViewsResponse(BaseModel):
    stream_feature_views: typing.List[StreamFeatureView] = Field(
        default_factory=list, alias="featureViews"
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class GetOnDemandFeatureViewRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListOnDemandFeatureViewsRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListOnDemandFeatureViewsResponse(BaseModel):
    on_demand_feature_views: typing.List[OnDemandFeatureView] = Field(
        default_factory=list, alias="featureViews"
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class ApplyFeatureServiceRequest(BaseModel):
    feature_service: FeatureService = Field(default_factory=FeatureService)
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetFeatureServiceRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListFeatureServicesRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    feature_view: str = Field(default="")
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListFeatureServicesResponse(BaseModel):
    feature_services: typing.List[FeatureService] = Field(
        default_factory=list, alias="featureServices"
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeleteFeatureServiceRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    commit: bool = Field(default=False)


class ApplySavedDatasetRequest(BaseModel):
    saved_dataset: SavedDataset = Field(default_factory=SavedDataset)
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetSavedDatasetRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListSavedDatasetsRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListSavedDatasetsResponse(BaseModel):
    saved_datasets: typing.List[SavedDataset] = Field(
        default_factory=list, alias="savedDatasets"
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeleteSavedDatasetRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    commit: bool = Field(default=False)


class ApplyValidationReferenceRequest(BaseModel):
    validation_reference: ValidationReference = Field(
        default_factory=ValidationReference
    )
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetValidationReferenceRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListValidationReferencesRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListValidationReferencesResponse(BaseModel):
    validation_references: typing.List[ValidationReference] = Field(
        default_factory=list
    )
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeleteValidationReferenceRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    commit: bool = Field(default=False)


class ApplyPermissionRequest(BaseModel):
    permission: Permission = Field(default_factory=Permission)
    project: str = Field(default="")
    commit: bool = Field(default=False)


class GetPermissionRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListPermissionsRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListPermissionsResponse(BaseModel):
    permissions: typing.List[Permission] = Field(default_factory=list)
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeletePermissionRequest(BaseModel):
    name: str = Field(default="")
    project: str = Field(default="")
    commit: bool = Field(default=False)


class ApplyProjectRequest(BaseModel):
    project: Project = Field(default_factory=Project)
    commit: bool = Field(default=False)


class GetProjectRequest(BaseModel):
    name: str = Field(default="")
    allow_cache: bool = Field(default=False)


class ListProjectsRequest(BaseModel):
    allow_cache: bool = Field(default=False)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListProjectsResponse(BaseModel):
    projects: typing.List[Project] = Field(default_factory=list)
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class DeleteProjectRequest(BaseModel):
    name: str = Field(default="")
    commit: bool = Field(default=False)


class EntityReference(BaseModel):
    type: str = Field(
        default=""
    )  # "dataSource", "entity", "featureView", "featureService"
    name: str = Field(default="")


class EntityRelation(BaseModel):
    source: EntityReference = Field(default_factory=EntityReference)
    target: EntityReference = Field(default_factory=EntityReference)


class GetRegistryLineageRequest(BaseModel):
    project: str = Field(default="")
    allow_cache: bool = Field(default=False)
    filter_object_type: str = Field(default="")
    filter_object_name: str = Field(default="")
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class GetRegistryLineageResponse(BaseModel):
    relationships: typing.List[EntityRelation] = Field(default_factory=list)
    indirect_relationships: typing.List[EntityRelation] = Field(default_factory=list)
    relationships_pagination: PaginationMetadata = Field(
        default_factory=PaginationMetadata
    )
    indirect_relationships_pagination: PaginationMetadata = Field(
        default_factory=PaginationMetadata
    )


class GetObjectRelationshipsRequest(BaseModel):
    project: str = Field(default="")
    object_type: str = Field(default="")
    object_name: str = Field(default="")
    include_indirect: bool = Field(default=False)
    allow_cache: bool = Field(default=False)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class GetObjectRelationshipsResponse(BaseModel):
    relationships: typing.List[EntityRelation] = Field(default_factory=list)
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class Feature(BaseModel):
    """
    Feature messages
    """

    name: str = Field(default="")
    feature_view: str = Field(default="")
    type: str = Field(default="")
    description: str = Field(default="")
    owner: str = Field(default="")
    created_timestamp: datetime = Field(default_factory=datetime.now)
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)


class ListFeaturesRequest(BaseModel):
    project: str = Field(default="")
    feature_view: str = Field(default="")
    name: str = Field(default="")
    allow_cache: bool = Field(default=False)
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sorting: SortingParams = Field(default_factory=SortingParams)


class ListFeaturesResponse(BaseModel):
    features: typing.List[Feature] = Field(default_factory=list)
    pagination: PaginationMetadata = Field(default_factory=PaginationMetadata)


class GetFeatureRequest(BaseModel):
    project: str = Field(default="")
    feature_view: str = Field(default="")
    name: str = Field(default="")
    allow_cache: bool = Field(default=False)
