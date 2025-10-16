import json
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from feast.api.registry.rest.feature_views import _extract_feature_view_from_any
from feast.api.registry.rest.rest_utils import (
    get_pagination_params,
    get_sorting_params,
    grpc_call,
    paginate_and_sort,
)
from feast.protos.feast.registry import RegistryServer_pb2


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


def get_metrics_router(grpc_handler, server=None) -> APIRouter:
    router = APIRouter()

    @router.get("/metrics/resource_counts", tags=["Metrics"])
    async def resource_counts(
        project: Optional[str] = Query(
            None, description="Project name to filter resource counts"
        ),
        allow_cache: bool = Query(True),
    ):
        def count_resources_for_project(project_name: str):
            entities = grpc_call(
                grpc_handler.ListEntities,
                RegistryServer_pb2.ListEntitiesRequest(
                    project=project_name, allow_cache=allow_cache
                ),
            )
            data_sources = grpc_call(
                grpc_handler.ListDataSources,
                RegistryServer_pb2.ListDataSourcesRequest(
                    project=project_name, allow_cache=allow_cache
                ),
            )
            try:
                saved_datasets = grpc_call(
                    grpc_handler.ListSavedDatasets,
                    RegistryServer_pb2.ListSavedDatasetsRequest(
                        project=project_name, allow_cache=allow_cache
                    ),
                )
            except Exception:
                saved_datasets = {"savedDatasets": []}
            try:
                features = grpc_call(
                    grpc_handler.ListFeatures,
                    RegistryServer_pb2.ListFeaturesRequest(
                        project=project_name, allow_cache=allow_cache
                    ),
                )
            except Exception:
                features = {"features": []}
            try:
                feature_views = grpc_call(
                    grpc_handler.ListAllFeatureViews,
                    RegistryServer_pb2.ListAllFeatureViewsRequest(
                        project=project_name, allow_cache=allow_cache
                    ),
                )
            except Exception:
                feature_views = {"featureViews": []}
            try:
                feature_services = grpc_call(
                    grpc_handler.ListFeatureServices,
                    RegistryServer_pb2.ListFeatureServicesRequest(
                        project=project_name, allow_cache=allow_cache
                    ),
                )
            except Exception:
                feature_services = {"featureServices": []}
            return {
                "entities": len(entities.get("entities", [])),
                "dataSources": len(data_sources.get("dataSources", [])),
                "savedDatasets": len(saved_datasets.get("savedDatasets", [])),
                "features": len(features.get("features", [])),
                "featureViews": len(feature_views.get("featureViews", [])),
                "featureServices": len(feature_services.get("featureServices", [])),
            }

        if project:
            counts = count_resources_for_project(project)
            return {"project": project, "counts": counts}
        else:
            # List all projects via gRPC
            projects_resp = grpc_call(
                grpc_handler.ListProjects,
                RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
            )
            all_projects = [
                p["spec"]["name"] for p in projects_resp.get("projects", [])
            ]
            all_counts = {}
            total_counts = {
                "entities": 0,
                "dataSources": 0,
                "savedDatasets": 0,
                "features": 0,
                "featureViews": 0,
                "featureServices": 0,
            }
            for project_name in all_projects:
                counts = count_resources_for_project(project_name)
                all_counts[project_name] = counts
                for k in total_counts:
                    total_counts[k] += counts[k]
            return {"total": total_counts, "perProject": all_counts}

    @router.get(
        "/metrics/popular_tags", tags=["Metrics"], response_model=PopularTagsResponse
    )
    async def popular_tags(
        project: Optional[str] = Query(
            None,
            description="Project name for popular tags (optional, returns all projects if not specified)",
        ),
        limit: int = Query(4, description="Number of popular tags to return"),
        allow_cache: bool = Query(default=True),
    ):
        """
        Discover Feature Views by popular tags. Returns the most popular tags
        (tags assigned to maximum number of feature views) with their associated feature views.
        If no project is specified, returns popular tags across all projects.
        """

        def build_tag_collection(
            feature_views: List[Dict],
        ) -> Dict[str, Dict[str, List[Dict]]]:
            """Build a collection of tags grouped by tag key and tag value."""
            tag_collection: Dict[str, Dict[str, List[Dict]]] = {}

            for fv in feature_views:
                tags = fv.get("spec", {}).get("tags", {})
                if not tags:
                    continue

                for tag_key, tag_value in tags.items():
                    if tag_key not in tag_collection:
                        tag_collection[tag_key] = {}

                    if tag_value not in tag_collection[tag_key]:
                        tag_collection[tag_key][tag_value] = []

                    tag_collection[tag_key][tag_value].append(fv)

            return tag_collection

        def find_most_popular_tags(
            tag_collection: Dict[str, Dict[str, List[Dict]]],
        ) -> List[Dict]:
            """Find the most popular tags based on total feature view count."""
            tag_popularity = []

            for tag_key, tag_values_map in tag_collection.items():
                for tag_value, fv_entries in tag_values_map.items():
                    total_feature_views = len(fv_entries)
                    tag_popularity.append(
                        {
                            "tag_key": tag_key,
                            "tag_value": tag_value,
                            "feature_views": fv_entries,
                            "total_feature_views": total_feature_views,
                        }
                    )

            return sorted(
                tag_popularity,
                key=lambda x: (x["total_feature_views"], x["tag_key"]),
                reverse=True,
            )

        def get_feature_views_for_project(project_name: str) -> List[Dict]:
            """Get feature views for a specific project."""
            req = RegistryServer_pb2.ListAllFeatureViewsRequest(
                project=project_name,
                allow_cache=allow_cache,
            )
            response = grpc_call(grpc_handler.ListAllFeatureViews, req)
            any_feature_views = response.get("featureViews", [])
            feature_views = []
            for any_feature_view in any_feature_views:
                feature_view = _extract_feature_view_from_any(any_feature_view)
                if feature_view:
                    feature_view["project"] = project_name
                    feature_views.append(feature_view)
            return feature_views

        try:
            if project:
                feature_views = get_feature_views_for_project(project)
            else:
                projects_resp = grpc_call(
                    grpc_handler.ListProjects,
                    RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
                )
                projects = projects_resp.get("projects", [])
                feature_views = []
                for project_info in projects:
                    project_name = project_info["spec"]["name"]
                    project_feature_views = get_feature_views_for_project(project_name)
                    feature_views.extend(project_feature_views)

            if not feature_views:
                return PopularTagsResponse(
                    popular_tags=[],
                    metadata=PopularTagsMetadata(
                        totalFeatureViews=0,
                        totalTags=0,
                        limit=limit,
                    ),
                )

            tag_collection = build_tag_collection(feature_views)

            if not tag_collection:
                return PopularTagsResponse(
                    popular_tags=[],
                    metadata=PopularTagsMetadata(
                        totalFeatureViews=len(feature_views),
                        totalTags=0,
                        limit=limit,
                    ),
                )
            popular_tags = find_most_popular_tags(tag_collection)
            top_popular_tags = popular_tags[:limit]
            formatted_tags = []
            for tag_info in top_popular_tags:
                feature_view_infos = [
                    FeatureViewInfo(
                        name=fv.get("spec", {}).get("name", "unknown"),
                        project=fv.get("project", "unknown"),
                    )
                    for fv in tag_info["feature_views"]
                ]

                formatted_tag = PopularTagInfo(
                    tag_key=tag_info["tag_key"],
                    tag_value=tag_info["tag_value"],
                    feature_views=feature_view_infos,
                    total_feature_views=tag_info["total_feature_views"],
                )
                formatted_tags.append(formatted_tag)

            return PopularTagsResponse(
                popular_tags=formatted_tags,
                metadata=PopularTagsMetadata(
                    totalFeatureViews=len(feature_views),
                    totalTags=len(popular_tags),
                    limit=limit,
                ),
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate popular tags: {str(e)}",
            )

    @router.get("/metrics/recently_visited", tags=["Metrics"])
    async def recently_visited(
        request: Request,
        project: Optional[str] = Query(
            None, description="Project name to filter recent visits"
        ),
        object_type: Optional[str] = Query(
            None,
            alias="object",
            description="Object type to filter recent visits (e.g., entities, features)",
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        user = None
        if hasattr(request.state, "user"):
            user = getattr(request.state, "user", None)
        if not user:
            user = "anonymous"
        key = f"recently_visited_{user}"
        visits = []
        if project:
            try:
                visits_json = (
                    server.registry.get_project_metadata(project, key)
                    if server
                    else None
                )
                visits = json.loads(visits_json) if visits_json else []
            except Exception:
                visits = []
        else:
            try:
                if server:
                    projects_resp = grpc_call(
                        grpc_handler.ListProjects,
                        RegistryServer_pb2.ListProjectsRequest(allow_cache=True),
                    )
                    all_projects = [
                        p["spec"]["name"] for p in projects_resp.get("projects", [])
                    ]
                    for project_name in all_projects:
                        try:
                            visits_json = server.registry.get_project_metadata(
                                project_name, key
                            )
                            if visits_json:
                                project_visits = json.loads(visits_json)
                                visits.extend(project_visits)
                        except Exception:
                            continue
                    visits = sorted(
                        visits, key=lambda x: x.get("timestamp", ""), reverse=True
                    )
            except Exception:
                visits = []
        if object_type:
            visits = [v for v in visits if v.get("object") == object_type]

        server_limit = getattr(server, "recent_visits_limit", 100) if server else 100
        visits = visits[-server_limit:]

        page = pagination_params.get("page", 0)
        limit = pagination_params.get("limit", 0)
        sort_by = sorting_params.get("sort_by")
        sort_order = sorting_params.get("sort_order", "asc")

        if page == 0 and limit == 0:
            if sort_by:
                visits = sorted(
                    visits,
                    key=lambda x: x.get(sort_by, ""),
                    reverse=(sort_order == "desc"),
                )
            return {"visits": visits, "pagination": {"totalCount": len(visits)}}
        else:
            if page == 0:
                page = 1
            if limit == 0:
                limit = 50
            paged_visits, pagination = paginate_and_sort(
                visits, page, limit, sort_by, sort_order
            )
            return {
                "visits": paged_visits,
                "pagination": pagination,
            }

    return router
