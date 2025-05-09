import logging
from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import grpc_call, parse_tags
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


def get_data_source_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/data_sources")
    def list_data_sources(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
    ):
        req = RegistryServer_pb2.ListDataSourcesRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListDataSources, req)
        return {"data_sources": response.get("dataSources", [])}

    @router.get("/data_sources/{name}")
    def get_data_source(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
    ):
        req = RegistryServer_pb2.GetDataSourceRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        return grpc_call(grpc_handler.GetDataSource, req)

    return router
