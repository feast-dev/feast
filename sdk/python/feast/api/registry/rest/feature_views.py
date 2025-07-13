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
