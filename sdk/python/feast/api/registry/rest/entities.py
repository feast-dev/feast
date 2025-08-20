import logging

from fastapi import APIRouter, Depends, Query

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

    return router
