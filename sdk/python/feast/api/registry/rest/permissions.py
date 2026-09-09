from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
)
from feast.protos.feast.core.Permission_pb2 import Permission as PermissionProto
from feast.protos.feast.core.Permission_pb2 import PermissionSpec as PermissionSpecProto
from feast.protos.feast.core.Policy_pb2 import (
    CombinedGroupNamespacePolicy as CombinedGroupNamespacePolicyProto,
)
from feast.protos.feast.core.Policy_pb2 import GroupBasedPolicy as GroupBasedPolicyProto
from feast.protos.feast.core.Policy_pb2 import (
    NamespaceBasedPolicy as NamespaceBasedPolicyProto,
)
from feast.protos.feast.core.Policy_pb2 import Policy as PolicyProto
from feast.protos.feast.core.Policy_pb2 import RoleBasedPolicy as RoleBasedPolicyProto
from feast.protos.feast.registry import RegistryServer_pb2


class RoleBasedPolicyModel(BaseModel):
    roles: List[str]


class GroupBasedPolicyModel(BaseModel):
    groups: List[str]


class NamespaceBasedPolicyModel(BaseModel):
    namespaces: List[str]


class CombinedGroupNamespacePolicyModel(BaseModel):
    groups: List[str]
    namespaces: List[str]


class PolicyModel(BaseModel):
    role_based_policy: Optional[RoleBasedPolicyModel] = None
    group_based_policy: Optional[GroupBasedPolicyModel] = None
    namespace_based_policy: Optional[NamespaceBasedPolicyModel] = None
    combined_group_namespace_policy: Optional[CombinedGroupNamespacePolicyModel] = None


class ApplyPermissionRequestBody(BaseModel):
    name: str
    project: str
    types: List[str] = []
    name_patterns: List[str] = []
    actions: List[str] = []
    policy: PolicyModel
    tags: Optional[Dict[str, str]] = {}
    required_tags: Optional[Dict[str, str]] = {}


def _build_policy_proto(policy: PolicyModel) -> PolicyProto:
    if policy.role_based_policy:
        return PolicyProto(
            role_based_policy=RoleBasedPolicyProto(roles=policy.role_based_policy.roles)
        )
    elif policy.group_based_policy:
        return PolicyProto(
            group_based_policy=GroupBasedPolicyProto(
                groups=policy.group_based_policy.groups
            )
        )
    elif policy.namespace_based_policy:
        return PolicyProto(
            namespace_based_policy=NamespaceBasedPolicyProto(
                namespaces=policy.namespace_based_policy.namespaces
            )
        )
    elif policy.combined_group_namespace_policy:
        return PolicyProto(
            combined_group_namespace_policy=CombinedGroupNamespacePolicyProto(
                groups=policy.combined_group_namespace_policy.groups,
                namespaces=policy.combined_group_namespace_policy.namespaces,
            )
        )
    return PolicyProto()


_TYPE_NAME_TO_ENUM = {
    "FEATURE_VIEW": PermissionSpecProto.Type.FEATURE_VIEW,
    "ON_DEMAND_FEATURE_VIEW": PermissionSpecProto.Type.ON_DEMAND_FEATURE_VIEW,
    "BATCH_FEATURE_VIEW": PermissionSpecProto.Type.BATCH_FEATURE_VIEW,
    "STREAM_FEATURE_VIEW": PermissionSpecProto.Type.STREAM_FEATURE_VIEW,
    "ENTITY": PermissionSpecProto.Type.ENTITY,
    "FEATURE_SERVICE": PermissionSpecProto.Type.FEATURE_SERVICE,
    "DATA_SOURCE": PermissionSpecProto.Type.DATA_SOURCE,
    "VALIDATION_REFERENCE": PermissionSpecProto.Type.VALIDATION_REFERENCE,
    "SAVED_DATASET": PermissionSpecProto.Type.SAVED_DATASET,
    "PERMISSION": PermissionSpecProto.Type.PERMISSION,
    "PROJECT": PermissionSpecProto.Type.PROJECT,
    "LABEL_VIEW": PermissionSpecProto.Type.LABEL_VIEW,
}

_ACTION_NAME_TO_ENUM = {
    "CREATE": PermissionSpecProto.AuthzedAction.CREATE,
    "DESCRIBE": PermissionSpecProto.AuthzedAction.DESCRIBE,
    "UPDATE": PermissionSpecProto.AuthzedAction.UPDATE,
    "DELETE": PermissionSpecProto.AuthzedAction.DELETE,
    "READ_ONLINE": PermissionSpecProto.AuthzedAction.READ_ONLINE,
    "READ_OFFLINE": PermissionSpecProto.AuthzedAction.READ_OFFLINE,
    "WRITE_ONLINE": PermissionSpecProto.AuthzedAction.WRITE_ONLINE,
    "WRITE_OFFLINE": PermissionSpecProto.AuthzedAction.WRITE_OFFLINE,
}


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

    @router.post("/permissions", status_code=201)
    def apply_permission(body: ApplyPermissionRequestBody):
        types = [_TYPE_NAME_TO_ENUM[t] for t in body.types if t in _TYPE_NAME_TO_ENUM]
        actions = [
            _ACTION_NAME_TO_ENUM[a] for a in body.actions if a in _ACTION_NAME_TO_ENUM
        ]
        policy_proto = _build_policy_proto(body.policy)

        permission_spec = PermissionSpecProto(
            name=body.name,
            types=types,
            name_patterns=body.name_patterns,
            actions=actions,
            policy=policy_proto,
            tags=body.tags or {},
            required_tags=body.required_tags or {},
        )
        permission_proto = PermissionProto(spec=permission_spec)

        req = RegistryServer_pb2.ApplyPermissionRequest(
            permission=permission_proto,
            project=body.project,
            commit=True,
        )
        grpc_call(grpc_handler.ApplyPermission, req)

        return JSONResponse(
            status_code=201,
            content={
                "name": body.name,
                "project": body.project,
                "status": "applied",
            },
        )

    @router.delete("/permissions/{name}")
    def delete_permission(
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.DeletePermissionRequest(
            name=name,
            project=project,
            commit=True,
        )
        grpc_call(grpc_handler.DeletePermission, req)

        return {"name": name, "project": project, "status": "deleted"}

    return router
