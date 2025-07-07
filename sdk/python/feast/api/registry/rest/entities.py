import logging

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_pagination_params,
    get_sorting_params,
    grpc_call,
)
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


def get_entity_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/entities")
    def list_entities(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListEntitiesRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )

        return grpc_call(grpc_handler.ListEntities, req)

    @router.get("/entities/{name}")
    def get_entity(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
    ):
        req = RegistryServer_pb2.GetEntityRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        return grpc_call(grpc_handler.GetEntity, req)

    return router
