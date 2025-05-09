from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import grpc_call, parse_tags
from feast.protos.feast.registry import RegistryServer_pb2


def get_feature_service_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_services")
    def list_feature_services(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
    ):
        req = RegistryServer_pb2.ListFeatureServicesRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        return grpc_call(grpc_handler.ListFeatureServices, req)

    @router.get("/feature_services/{name}")
    def get_feature_service(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
    ):
        req = RegistryServer_pb2.GetFeatureServiceRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        return grpc_call(grpc_handler.GetFeatureService, req)

    return router
