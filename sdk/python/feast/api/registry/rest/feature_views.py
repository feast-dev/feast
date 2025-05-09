from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import grpc_call, parse_tags
from feast.registry_server import RegistryServer_pb2


def get_feature_view_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_views/{name}")
    def get_any_feature_view(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetAnyFeatureView, req)
        return response.get("anyFeatureView", {})

    @router.get("/feature_views")
    def list_all_feature_views(
        project: str = Query(...),
        allow_cache: bool = Query(True),
        tags: Dict[str, str] = Depends(parse_tags),
    ):
        req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        return grpc_call(grpc_handler.ListAllFeatureViews, req)

    return router
