from typing import Dict, List, Optional

from fastapi import HTTPException, Query
from google.protobuf.json_format import MessageToDict

from feast.errors import FeastObjectNotFoundException
from feast.protos.feast.registry import RegistryServer_pb2


def grpc_call(handler_fn, request):
    """
    Wrapper to invoke gRPC method with context=None and handle common errors.
    """
    try:
        response = handler_fn(request, context=None)
        return MessageToDict(response)
    except FeastObjectNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")


def get_object_relationships(
    grpc_handler,
    object_type: str,
    object_name: str,
    project: str,
    allow_cache: bool = True,
) -> List:
    """
    Get relationships for a specific object.

    Args:
        grpc_handler: The gRPC handler to use for calls
        object_type: Type of object (dataSource, entity, featureView, featureService)
        object_name: Name of the object
        project: Project name
        allow_cache: Whether to allow cached data

    Returns:
        List containing relationships for the object (both direct and indirect)
    """
    try:
        req = RegistryServer_pb2.GetObjectRelationshipsRequest(
            project=project,
            object_type=object_type,
            object_name=object_name,
            include_indirect=True,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetObjectRelationships, req)
        return response.get("relationships", [])
    except Exception:
        # If relationships can't be retrieved, return empty list rather than failing
        return []


def get_relationships_for_objects(
    grpc_handler,
    objects: List[Dict],
    object_type: str,
    project: str,
    allow_cache: bool = True,
) -> Dict[str, List]:
    """
    Get relationships for multiple objects efficiently.

    Args:
        grpc_handler: The gRPC handler to use for calls
        objects: List of objects to get relationships for
        object_type: Type of objects (dataSource, entity, featureView, featureService)
        project: Project name
        allow_cache: Whether to allow cached data

    Returns:
        Dictionary mapping object names to their relationships (both direct and indirect)
    """
    relationships_map = {}

    for obj in objects:
        # Extract object name from different possible structures
        obj_name = None
        if isinstance(obj, dict):
            # Consistent name extraction logic for all object types
            obj_name = (
                obj.get("name")
                or obj.get("spec", {}).get("name")
                or obj.get("meta", {}).get("name")
            )

        if obj_name:
            relationships_map[obj_name] = get_object_relationships(
                grpc_handler,
                object_type,
                obj_name,
                project,
                allow_cache,
            )

    return relationships_map


def parse_tags(tags: List[str] = Query(default=[])) -> Dict[str, str]:
    """
    Parses query strings like ?tags=key1:value1&tags=key2:value2 into a dict.
    """
    parsed_tags = {}
    for tag in tags:
        if ":" not in tag:
            continue
        key, value = tag.split(":", 1)
        parsed_tags[key] = value
    return parsed_tags


def get_pagination_params(
    page: Optional[int] = Query(None, ge=1),
    limit: Optional[int] = Query(None, ge=1, le=100),
) -> dict:
    return {
        "page": page or 0,
        "limit": limit or 0,
    }


def get_sorting_params(
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query(None, pattern="^(asc|desc)$"),
) -> dict:
    return {
        "sort_by": sort_by or "",
        "sort_order": sort_order or "asc",
    }


def create_grpc_pagination_params(
    pagination_params: dict,
) -> RegistryServer_pb2.PaginationParams:
    return RegistryServer_pb2.PaginationParams(
        page=pagination_params.get("page", 0),
        limit=pagination_params.get("limit", 0),
    )


def create_grpc_sorting_params(
    sorting_params: dict,
) -> RegistryServer_pb2.SortingParams:
    return RegistryServer_pb2.SortingParams(
        sort_by=sorting_params.get("sort_by", ""),
        sort_order=sorting_params.get("sort_order", "asc"),
    )
