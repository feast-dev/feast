"""
Pydantic response models for Feast REST API endpoints.

This file contains all response models for the Feast REST API, organized by the
endpoint file they correspond to. Each model represents the JSON structure
returned by specific API endpoints.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from feast.datamodels.feast.core.DataSource_p2p import DataSource
from feast.datamodels.feast.core.Entity_p2p import Entity, EntityMeta, EntitySpecV2
from feast.datamodels.feast.core.FeatureService_p2p import (
    FeatureService,
    FeatureServiceMeta,
    FeatureServiceSpec,
)
from feast.datamodels.feast.core.FeatureView_p2p import (
    FeatureView,
    FeatureViewMeta,
    FeatureViewSpec,
)
from feast.datamodels.feast.core.Permission_p2p import (
    Permission,
    PermissionMeta,
    PermissionSpec,
)
from feast.datamodels.feast.core.Project_p2p import Project, ProjectMeta, ProjectSpec
from feast.datamodels.feast.core.SavedDataset_p2p import (
    SavedDataset,
    SavedDatasetMeta,
    SavedDatasetSpec,
)
from feast.datamodels.feast.registry.RegistryServer_p2p import EntityRelation

# =============================================================================
# BASE Models
# =============================================================================


class PaginationMetadata(BaseModel):
    page: int = Field(default=0)
    limit: int = Field(default=0)
    totalCount: int = Field(default=0)
    totalPages: int = Field(default=0)
    hasNext: bool = Field(default=False)
    hasPrevious: bool = Field(default=False)


class EntityWithProject(Entity):
    project: str = Field(..., description="Project name")


class FeatureViewWithType(FeatureView):
    type: str = Field(..., description="Type of feature view")


class FeatureViewWithProject(FeatureView):
    project: str = Field(..., description="Project name")


class FeatureServiceWithProject(FeatureService):
    project: str = Field(..., description="Project name")


class SavedDatasetWithProject(SavedDataset):
    project: str = Field(..., description="Project name")


class EntityRelationWithProject(EntityRelation):
    project: str = Field(..., description="Project name")


# =============================================================================
# ENTITIES.PY - Entity endpoint responses
# =============================================================================


class EntityResponse(BaseModel):
    """Response for GET /entities/{name} - entities.py:get_entity"""

    spec: EntitySpecV2 = Field(..., description="Entity specification")
    meta: EntityMeta = Field(..., description="Entity metadata")
    data_sources: List[DataSource] = Field(
        default_factory=list, description="Related data sources", alias="dataSources"
    )
    relationships: Optional[List[EntityRelation]] = Field(
        None, description="Entity relationships"
    )
    feature_definition: Optional[str] = Field(
        None, description="Generated entity code", alias="featureDefinition"
    )


class ListEntitiesResponse(BaseModel):
    """Response for GET /entities - entities.py:list_entities"""

    entities: List[Entity] = Field(..., description="List of entities")
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Entity relationships map"
    )


class ListAllEntitiesResponse(BaseModel):
    """Response for GET /entities/all - entities.py:list_all_entities"""

    entities: List[EntityWithProject] = Field(
        ..., description="List of entities across all projects"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Entity relationships map"
    )


# =============================================================================
# FEATURE_VIEWS.PY - Feature View endpoint responses
# =============================================================================


class AnyFeatureViewResponse(BaseModel):
    """Response for GET /feature_views/{name} - feature_views.py:get_any_feature_view"""

    type: Optional[str] = Field(None, description="Type of feature view")
    spec: FeatureViewSpec = Field(..., description="Feature view specification")
    meta: FeatureViewMeta = Field(..., description="Feature view metadata")
    relationships: Optional[List[EntityRelation]] = Field(
        None, description="Feature view relationships"
    )
    feature_definition: Optional[str] = Field(
        None, description="Generated feature view code", alias="featureDefinition"
    )


class ListFeatureViewsAllResponse(BaseModel):
    """Response for GET /feature_views/all - feature_views.py:list_feature_views_all"""

    feature_views: List[FeatureViewWithProject] = Field(
        ..., description="List of feature views", alias="featureViews"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Feature view relationships map"
    )


class ListAllFeatureViewsResponse(BaseModel):
    """Response for GET /feature_views - feature_views.py:list_all_feature_views"""

    feature_views: List[FeatureViewWithType] = Field(
        ..., description="List of feature views", alias="featureViews"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Feature view relationships map"
    )


# =============================================================================
# FEATURE_SERVICES.PY - Feature Service endpoint responses
# =============================================================================


class FeatureServiceResponse(BaseModel):
    """Response for GET /feature_services/{name} - feature_services.py:get_feature_service"""

    spec: FeatureServiceSpec = Field(..., description="Feature service specification")
    meta: FeatureServiceMeta = Field(..., description="Feature service metadata")
    relationships: Optional[List[EntityRelation]] = Field(
        None, description="Feature service relationships"
    )
    feature_definition: Optional[str] = Field(
        None, description="Generated feature service code", alias="featureDefinition"
    )


class ListFeatureServicesResponse(BaseModel):
    """Response for GET /feature_services - feature_services.py:list_feature_services"""

    feature_services: List[FeatureService] = Field(
        ..., description="List of feature services", alias="featureServices"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Feature service relationships map"
    )


class ListFeatureServicesAllResponse(BaseModel):
    """Response for GET /feature_services/all - feature_services.py:list_feature_services_all"""

    feature_services: List[FeatureServiceWithProject] = Field(
        ..., description="List of feature services", alias="featureServices"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Feature service relationships map"
    )


# =============================================================================
# DATA_SOURCES.PY - Data Source endpoint responses
# =============================================================================


class DataSourceResponse(DataSource):
    """Response for GET /data_sources/{name} - data_sources.py:get_data_source"""

    relationships: Optional[List[EntityRelation]] = Field(
        None, description="Data source relationships"
    )
    feature_definition: Optional[str] = Field(
        None, description="Generated data source code", alias="featureDefinition"
    )


class ListDataSourcesResponse(BaseModel):
    """Response for GET /data_sources - data_sources.py:list_data_sources"""

    data_sources: List[Dict[str, Any]] = Field(
        ..., description="List of data sources", alias="dataSources"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Data source relationships map"
    )


class ListDataSourcesAllResponse(BaseModel):
    """Response for GET /data_sources/all - data_sources.py:list_data_sources_all"""

    data_sources: List[Dict[str, Any]] = Field(
        ..., description="List of data sources", alias="dataSources"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Data source relationships map"
    )


# =============================================================================
# SAVED_DATASETS.PY - Saved Dataset endpoint responses
# =============================================================================


class SavedDatasetResponse(BaseModel):
    """Response for GET /saved_datasets/{name} - saved_datasets.py:get_saved_dataset"""

    spec: SavedDatasetSpec = Field(..., description="Saved dataset specification")
    meta: SavedDatasetMeta = Field(..., description="Saved dataset metadata")
    relationships: Optional[List[EntityRelation]] = Field(
        None, description="Saved dataset relationships"
    )
    feature_definition: Optional[str] = Field(
        None, description="Generated saved dataset code", alias="featureDefinition"
    )


class ListSavedDatasetsResponse(BaseModel):
    """Response for GET /saved_datasets - saved_datasets.py:list_saved_datasets"""

    saved_datasets: List[SavedDataset] = Field(
        ..., description="List of saved datasets", alias="savedDatasets"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Saved dataset relationships map"
    )


class ListSavedDatasetsAllResponse(BaseModel):
    """Response for GET /saved_datasets/all - saved_datasets.py:list_saved_datasets_all"""

    saved_datasets: List[SavedDatasetWithProject] = Field(
        ..., description="List of saved datasets", alias="savedDatasets"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Saved dataset relationships map"
    )


# =============================================================================
# PERMISSIONS.PY - Permission endpoint responses
# =============================================================================


class PermissionResponse(BaseModel):
    """Response for GET /permissions/{name} - permissions.py:get_permission"""

    name: str = Field(..., description="Permission name")
    spec: PermissionSpec = Field(..., description="Permission specification")
    meta: PermissionMeta = Field(..., description="Permission metadata")
    relationships: Optional[List[EntityRelation]] = Field(
        None, description="Permission relationships"
    )


class ListPermissionsResponse(BaseModel):
    """Response for GET /permissions - permissions.py:list_permissions"""

    permissions: List[Permission] = Field(..., description="List of permissions")
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Permission relationships map"
    )


# =============================================================================
# PROJECTS.PY - Project endpoint responses
# =============================================================================


class ProjectResponse(BaseModel):
    """Response for GET /projects/{name} - projects.py:get_project"""

    spec: ProjectSpec = Field(..., description="Project specification")
    meta: ProjectMeta = Field(..., description="Project metadata")


class ListProjectsResponse(BaseModel):
    """Response for GET /projects - projects.py:list_projects"""

    projects: List[Project] = Field(..., description="List of projects")
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")


class ProjectErrorResponse(BaseModel):
    """Error response for project endpoints - projects.py:list_projects"""

    error: str = Field(..., description="Error message")


# =============================================================================
# LINEAGE.PY - Lineage endpoint responses
# =============================================================================


class RegistryObjects(BaseModel):
    """Registry objects collection."""

    entities: List[Entity] = Field(default_factory=list, description="List of entities")
    data_sources: List[Dict[str, Any]] = Field(
        default_factory=list, description="List of data sources", alias="dataSources"
    )
    feature_views: List[FeatureView] = Field(
        default_factory=list, description="List of feature views", alias="featureViews"
    )
    feature_services: List[FeatureService] = Field(
        default_factory=list,
        description="List of feature services",
        alias="featureServices",
    )
    features: List[Dict[str, Any]] = Field(
        default_factory=list, description="List of features"
    )


class RegistryPagination(BaseModel):
    """Pagination metadata for registry objects."""

    entities: PaginationMetadata = Field(
        default_factory=PaginationMetadata, description="Entity pagination"
    )
    data_sources: PaginationMetadata = Field(
        default_factory=PaginationMetadata,
        description="Data source pagination",
        alias="dataSources",
    )
    feature_views: PaginationMetadata = Field(
        default_factory=PaginationMetadata,
        description="Feature view pagination",
        alias="featureViews",
    )
    feature_services: PaginationMetadata = Field(
        default_factory=PaginationMetadata,
        description="Feature service pagination",
        alias="featureServices",
    )
    features: PaginationMetadata = Field(
        default_factory=PaginationMetadata, description="Feature pagination"
    )
    relationships: PaginationMetadata = Field(
        default_factory=PaginationMetadata, description="Relationships pagination"
    )
    indirect_relationships: PaginationMetadata = Field(
        default_factory=PaginationMetadata,
        description="Indirect relationships pagination",
        alias="indirectRelationships",
    )


class CompleteRegistryDataResponse(BaseModel):
    """Response for GET /lineage/complete - lineage.py:get_complete_registry_data"""

    project: str = Field(..., description="Project name")
    objects: RegistryObjects = Field(..., description="Registry objects")
    relationships: List[EntityRelation] = Field(..., description="Direct relationships")
    indirect_relationships: List[EntityRelation] = Field(
        ..., description="Indirect relationships", alias="indirectRelationships"
    )
    pagination: RegistryPagination = Field(..., description="Pagination metadata")


class ProjectRegistryData(BaseModel):
    """Registry data for a single project."""

    project: str = Field(..., description="Project name")
    objects: RegistryObjects = Field(..., description="Registry objects")
    relationships: List[EntityRelation] = Field(..., description="Direct relationships")
    indirect_relationships: List[EntityRelation] = Field(
        ..., description="Indirect relationships", alias="indirectRelationships"
    )


class RegistryLineageAllResponse(BaseModel):
    """Response for GET /lineage/registry/all - lineage.py:get_registry_lineage_all"""

    relationships: List[EntityRelationWithProject] = Field(
        ..., description="All direct relationships across projects"
    )
    indirect_relationships: List[EntityRelationWithProject] = Field(
        ...,
        description="All indirect relationships across projects",
        alias="indirect_relationships",
    )


class CompleteRegistryDataAllResponse(BaseModel):
    """Response for GET /lineage/complete/all - lineage.py:get_complete_registry_data_all"""

    projects: List[ProjectRegistryData] = Field(
        ..., description="Complete registry data for all projects"
    )


# =============================================================================
# FEATURES.PY - Feature endpoint responses
# =============================================================================


class GetFeatureResponse(BaseModel):
    """Response for GET /features/{feature_view}/{name} - features.py:get_feature"""

    name: str = Field(..., description="Feature name")
    featureView: str = Field(..., description="Feature view name", alias="featureView")
    type: str = Field(..., description="Feature type")
    featureDefinition: Optional[str] = Field(
        None, description="Generated feature code", alias="featureDefinition"
    )
    relationships: Optional[List[EntityRelation]] = Field(
        None, description="Feature relationships"
    )


class GetFeatureResponseWithProject(GetFeatureResponse):
    project: str = Field(..., description="Project name")


class ListFeaturesResponse(BaseModel):
    """Response for GET /features - features.py:list_features"""

    features: List[GetFeatureResponse] = Field(..., description="List of features")
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Feature relationships map"
    )


class ListFeaturesAllResponse(BaseModel):
    """Response for GET /features/all - features.py:list_features_all"""

    features: List[GetFeatureResponseWithProject] = Field(
        ..., description="List of features across all projects"
    )
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    relationships: Optional[Dict[str, List[EntityRelation]]] = Field(
        None, description="Feature relationships map"
    )


# =============================================================================
# METRICS.PY - Metrics endpoint responses
# =============================================================================


class ResourceCounts(BaseModel):
    """Resource counts for a project."""

    entities: int = Field(..., description="Number of entities")
    data_sources: int = Field(
        ..., description="Number of data sources", alias="dataSources"
    )
    saved_datasets: int = Field(
        ..., description="Number of saved datasets", alias="savedDatasets"
    )
    features: int = Field(..., description="Number of features")
    feature_views: int = Field(
        ..., description="Number of feature views", alias="featureViews"
    )
    feature_services: int = Field(
        ..., description="Number of feature services", alias="featureServices"
    )


class ProjectResourceCountsResponse(BaseModel):
    """Response for GET /metrics/resource_counts with project - metrics.py:resource_counts"""

    project: str = Field(..., description="Project name")
    counts: ResourceCounts = Field(..., description="Resource counts for the project")


class AllResourceCountsResponse(BaseModel):
    """Response for GET /metrics/resource_counts without project - metrics.py:resource_counts"""

    total: ResourceCounts = Field(
        ..., description="Total resource counts across all projects"
    )
    per_project: Dict[str, ResourceCounts] = Field(
        ..., description="Resource counts per project", alias="perProject"
    )


class FeatureViewInfo(BaseModel):
    """Feature view information in popular tags response."""

    name: str = Field(..., description="Name of the feature view")
    project: str = Field(..., description="Project name of the feature view")


class PopularTagInfo(BaseModel):
    """Popular tag information with associated feature views."""

    tag_key: str = Field(..., description="Tag key")
    tag_value: str = Field(..., description="Tag value")
    feature_views: List[FeatureViewInfo] = Field(
        ..., description="List of feature views with this tag"
    )
    total_feature_views: int = Field(
        ..., description="Total number of feature views with this tag"
    )


class PopularTagsMetadata(BaseModel):
    """Metadata for popular tags response."""

    totalFeatureViews: int = Field(
        ..., description="Total number of feature views processed"
    )
    totalTags: int = Field(..., description="Total number of unique tags found")
    limit: int = Field(..., description="Number of popular tags requested")


class PopularTagsResponse(BaseModel):
    """Response model for popular tags endpoint."""

    popular_tags: List[PopularTagInfo] = Field(
        ..., description="List of popular tags with their associated feature views"
    )
    metadata: PopularTagsMetadata = Field(
        ..., description="Metadata about the response"
    )


class RecentVisit(BaseModel):
    """Recent visit information."""

    object: Any = Field(..., description="Object type visited")
    object_name: Any = Field(..., description="Object name")
    project: str = Field(..., description="Project name")
    timestamp: str = Field(..., description="Visit timestamp")
    path: str = Field(..., description="Path visited")
    method: str = Field(..., description="Method used")
    user: str = Field(..., description="User who visited the object")


class RecentlyVisitedResponse(BaseModel):
    """Response for GET /metrics/recently_visited - metrics.py:recently_visited"""

    visits: List[RecentVisit] = Field(..., description="List of recent visits")
    pagination: Dict[str, Any] = Field(..., description="Pagination metadata")


# =============================================================================
# SEARCH.PY - Search endpoint responses
# =============================================================================


class SearchResult(BaseModel):
    """Individual search result."""

    type: Literal[
        "entity",
        "dataSource",
        "featureView",
        "featureService",
        "feature",
        "savedDataset",
    ] = Field(..., description="Type of resource")
    name: str = Field(..., description="Resource name")
    description: str = Field(default="", description="Resource description")
    project: str = Field(..., description="Project name")
    match_score: Optional[float] = Field(
        None, description="Search match score", alias="match_score"
    )
    featureView: Optional[str] = Field(
        None, description="Feature view name", alias="featureView"
    )


class SearchResponse(BaseModel):
    """Response for GET /search - search.py:search_resources"""

    query: str = Field(..., description="Search query string")
    projects_searched: List[str] = Field(
        ..., description="List of projects searched", alias="projects_searched"
    )
    results: List[SearchResult] = Field(..., description="Search results")
    pagination: PaginationMetadata = Field(..., description="Pagination metadata")
    errors: List[str] = Field(
        default_factory=list, description="List of errors encountered during search"
    )
