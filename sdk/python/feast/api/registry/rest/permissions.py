from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_pagination_params,
    get_sorting_params,
    grpc_call,
)
from feast.registry_server import RegistryServer_pb2


def get_permission_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/permissions/{name}")
    def get_permission(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetPermissionRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        return {"permission": grpc_call(grpc_handler.GetPermission, req)}

    @router.get("/permissions")
    def list_permissions(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListPermissionsRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        return grpc_call(grpc_handler.ListPermissions, req)

    return router
