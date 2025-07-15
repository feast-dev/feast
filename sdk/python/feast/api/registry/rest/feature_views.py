from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
    paginate_and_sort,
    parse_tags,
)
from feast.registry_server import RegistryServer_pb2


def _extract_feature_view_from_any(any_feature_view: dict) -> dict:
    """Extract the specific feature view type and data from an AnyFeatureView object.

    Args:
        any_feature_view: Dictionary containing the AnyFeatureView data

    Returns:
        Dictionary with 'type' and feature view data, or empty dict if no valid type found
    """
    for key, value in any_feature_view.items():
        if value:
            return {"type": key, **value}

    return {}


def get_feature_view_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_views/all")
    def list_feature_views_all(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature view"
        ),
    ):
        projects_resp = grpc_call(
            grpc_handler.ListProjects,
            RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
        )
        projects = projects_resp.get("projects", [])
        all_feature_views = []
        for project in projects:
            project_name = project["spec"]["name"]
            req = RegistryServer_pb2.ListAllFeatureViewsRequest(
                project=project_name,
                allow_cache=allow_cache,
            )
            response = grpc_call(grpc_handler.ListAllFeatureViews, req)
            any_feature_views = response.get("featureViews", [])
            for any_feature_view in any_feature_views:
                feature_view = _extract_feature_view_from_any(any_feature_view)
                if feature_view:
                    feature_view["project"] = project_name
                    all_feature_views.append(feature_view)
        paged_feature_views, pagination = paginate_and_sort(
            all_feature_views, page, limit, sort_by, sort_order
        )
        result = {
            "featureViews": paged_feature_views,
            "pagination": pagination,
        }
        if include_relationships:
            relationships_map = {}
            for project in projects:
                project_name = project["spec"]["name"]
                # Filter feature views for this project
                project_feature_views = [
                    fv for fv in all_feature_views if fv["project"] == project_name
                ]
                rels = get_relationships_for_objects(
                    grpc_handler,
                    project_feature_views,
                    "featureView",
                    project_name,
                    allow_cache,
                )
                relationships_map.update(rels)
            result["relationships"] = relationships_map
        return result

    @router.get("/feature_views/{name}")
    def get_any_feature_view(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this feature view"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetAnyFeatureView, req)
        any_feature_view = response.get("anyFeatureView", {})

        result = _extract_feature_view_from_any(any_feature_view)

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "featureView", name, project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.get("/feature_views")
    def list_all_feature_views(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature view"
        ),
        tags: Dict[str, str] = Depends(parse_tags),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListAllFeatureViews, req)
        any_feature_views = response.get("featureViews", [])

        # Extract the specific type of feature view from each AnyFeatureView
        feature_views = []
        for any_feature_view in any_feature_views:
            feature_view = _extract_feature_view_from_any(any_feature_view)
            if feature_view:
                feature_views.append(feature_view)

        result = {
            "featureViews": feature_views,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, feature_views, "featureView", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    return router
