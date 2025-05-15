import logging

from fastapi import APIRouter, Query

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


def get_entity_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/entities")
    def list_entities(
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.ListEntitiesRequest(project=project)
        response = grpc_call(grpc_handler.ListEntities, req)
        return {"entities": response.get("entities", [])}

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
