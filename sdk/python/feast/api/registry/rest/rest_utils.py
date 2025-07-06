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
    sort_order: Optional[str] = Query(None, regex="^(asc|desc)$"),
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
