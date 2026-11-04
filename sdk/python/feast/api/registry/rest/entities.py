import logging
from typing import Dict, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from feast.api.registry.rest.codegen_utils import render_entity_code
from feast.api.registry.rest.rest_utils import (
    aggregate_across_projects,
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
)
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


class ApplyEntityRequestBody(BaseModel):
    name: str
    project: str
    join_key: Optional[str] = None
    value_type: Optional[int] = 2
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = {}
    owner: Optional[str] = ""


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

    @router.get("/entities/all")
    def list_all_entities(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each entity"
        ),
    ):
        return aggregate_across_projects(
            grpc_handler=grpc_handler,
            list_method=grpc_handler.ListEntities,
            request_cls=RegistryServer_pb2.ListEntitiesRequest,
            response_key="entities",
            object_type="entity",
            allow_cache=allow_cache,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            include_relationships=include_relationships,
        )

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

        relationships = get_object_relationships(
            grpc_handler, "entity", name, project, allow_cache
        )
        ds_list_req = RegistryServer_pb2.ListDataSourcesRequest(
            project=project,
            allow_cache=allow_cache,
        )
        ds_list_resp = grpc_call(grpc_handler.ListDataSources, ds_list_req)
        ds_map = {ds["name"]: ds for ds in ds_list_resp.get("dataSources", [])}
        data_source_objs = []
        seen_ds_names = set()
        for rel in relationships:
            if rel.get("target", {}).get("type") == "dataSource":
                ds_name = rel["target"].get("name")
                if ds_name and ds_name not in seen_ds_names:
                    ds_obj = ds_map.get(ds_name)
                    if ds_obj:
                        data_source_objs.append(ds_obj)
                        seen_ds_names.add(ds_name)
        result["dataSources"] = data_source_objs

        if include_relationships:
            result["relationships"] = relationships

        if result:
            spec = result.get("spec", result)
            name = spec.get("name") or result.get("name") or "default_entity"
            join_keys = spec.get("joinKeys") or (
                [spec["joinKey"]] if "joinKey" in spec else []
            )

            context = {
                "name": name,
                "join_keys": join_keys,
                "description": spec.get("description", ""),
                "tags": spec.get("tags", {}),
            }
            result["featureDefinition"] = render_entity_code(context)
        return result

    @router.post("/entities", status_code=201)
    def apply_entity(body: ApplyEntityRequestBody):
        join_key = body.join_key if body.join_key else body.name

        spec = EntitySpecProto(
            name=body.name,
            value_type=body.value_type,
            join_key=join_key,
            description=body.description or "",
            tags=body.tags or {},
            owner=body.owner or "",
        )
        entity_proto = EntityProto(spec=spec)

        req = RegistryServer_pb2.ApplyEntityRequest(
            entity=entity_proto,
            project=body.project,
            commit=True,
        )
        grpc_call(grpc_handler.ApplyEntity, req)

        return JSONResponse(
            status_code=201,
            content={"name": body.name, "project": body.project, "status": "applied"},
        )

    @router.delete("/entities/{name}")
    def delete_entity(
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.DeleteEntityRequest(
            name=name,
            project=project,
            commit=True,
        )
        grpc_call(grpc_handler.DeleteEntity, req)

        return {"name": name, "project": project, "status": "deleted"}

    return router
