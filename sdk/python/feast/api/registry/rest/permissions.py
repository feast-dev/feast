from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
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
        include_relationships: bool = Query(
            False, description="Include relationships for this permission"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetPermissionRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        permission = grpc_call(grpc_handler.GetPermission, req)

        result = permission

        # Note: permissions may not have relationships in the traditional sense
        # but we include the functionality for consistency
        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "permission", name, project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.get("/permissions")
    def list_permissions(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        include_relationships: bool = Query(
            False, description="Include relationships for each permission"
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListPermissionsRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListPermissions, req)
        permissions = response.get("permissions", [])

        result = {
            "permissions": permissions,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, permissions, "permission", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    return router
