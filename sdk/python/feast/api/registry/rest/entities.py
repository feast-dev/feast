import logging

from fastapi import APIRouter, Query, Body
from typing import Dict, List, Optional, Any

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.registry import RegistryServer_pb2
from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config

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
    
    @router.post("/entities")
    def create_entity(
        entity_data: Dict[str, Any] = Body(...),
    ):
        try:
            name = entity_data.get("name")
            value_type_str = entity_data.get("valueType")
            join_key = entity_data.get("joinKey", name)
            description = entity_data.get("description", "")
            tags = entity_data.get("tags", {})
            owner = entity_data.get("owner", "")
            project = entity_data.get("project", "default")
            
            from feast.value_type import ValueType
            try:
                value_type = ValueType[value_type_str]
            except (KeyError, TypeError):
                return {"status": "error", "detail": f"Invalid value type: {value_type_str}"}
            
            entity = Entity(
                name=name,
                value_type=value_type,
                join_key=join_key,
                description=description,
                tags=tags,
                owner=owner,
            )
            
            repo_config = load_repo_config()
            fs = FeatureStore(config=repo_config)
            fs.apply(entity)
            
            return {"status": "success", "message": f"Entity {name} created successfully"}
        except Exception as e:
            logger.exception(f"Error creating entity: {str(e)}")
            return {"status": "error", "detail": str(e)}

    return router
