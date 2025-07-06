import logging

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
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


def get_entity_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/entities")
    def list_entities(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        include_relationships: bool = Query(
            False, description="Include relationships for each entity"
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListEntitiesRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListEntities, req)
        entities = response.get("entities", [])

        result = {
            "entities": entities,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, entities, "entity", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.get("/entities/{name}")
    def get_entity(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this entity"
        ),
        allow_cache: bool = Query(default=True),
    ):
        req = RegistryServer_pb2.GetEntityRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        entity = grpc_call(grpc_handler.GetEntity, req)

        result = entity

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "entity", name, project, allow_cache
            )
            result["relationships"] = relationships

        return result

    return router
