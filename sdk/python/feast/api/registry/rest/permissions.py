import logging
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Query, Body

from feast.api.registry.rest.rest_utils import grpc_call
from feast.registry_server import RegistryServer_pb2
from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config

logger = logging.getLogger(__name__)


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
    
    @router.post("/permissions")
    def create_permission(
        permission_data: Dict[str, Any] = Body(...),
    ):
        try:
            name = permission_data.get("name")
            description = permission_data.get("description", "")
            principal = permission_data.get("principal")
            resource = permission_data.get("resource")
            action = permission_data.get("action")
            tags = permission_data.get("tags", {})
            owner = permission_data.get("owner", "")
            project = permission_data.get("project", "default")
            
            repo_config = load_repo_config()
            fs = FeatureStore(config=repo_config)
            
            permission = {
                "name": name,
                "description": description,
                "principal": principal,
                "resource": resource,
                "action": action,
                "tags": tags,
                "owner": owner,
            }
            
            fs.apply_permission(permission)
            
            return {"status": "success", "message": f"Permission {name} created successfully"}
        except Exception as e:
            logger.exception(f"Error creating permission: {str(e)}")
            return {"status": "error", "detail": str(e)}

    return router
