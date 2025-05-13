from fastapi import APIRouter, Query

from feast.api.registry.rest.rest_utils import grpc_call
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
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.ListPermissionsRequest(
            project=project,
            allow_cache=allow_cache,
        )
        return grpc_call(grpc_handler.ListPermissions, req)

    return router
