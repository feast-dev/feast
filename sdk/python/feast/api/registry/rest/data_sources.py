import logging
from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.codegen_utils import (
    render_data_source_code,
    render_push_source_code,
    render_request_source_code,
)
from feast.api.registry.rest.rest_utils import (
    aggregate_across_projects,
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
    parse_tags,
)
from feast.protos.feast.registry import RegistryServer_pb2
from feast.type_map import _convert_value_type_str_to_value_type
from feast.types import from_value_type

logger = logging.getLogger(__name__)


def get_data_source_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/data_sources")
    def list_data_sources(
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for each data source"
        ),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListDataSourcesRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListDataSources, req)
        data_sources = response.get("dataSources", [])

        result = {
            "dataSources": data_sources,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, data_sources, "dataSource", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.get("/data_sources/all")
    def list_data_sources_all(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each data source"
        ),
    ):
        return aggregate_across_projects(
            grpc_handler=grpc_handler,
            list_method=grpc_handler.ListDataSources,
            request_cls=RegistryServer_pb2.ListDataSourcesRequest,
            response_key="dataSources",
            object_type="dataSource",
            allow_cache=allow_cache,
            page=page,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            include_relationships=include_relationships,
        )

    @router.get("/data_sources/{name}")
    def get_data_source(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this data source"
        ),
        allow_cache: bool = Query(default=True),
    ):
        req = RegistryServer_pb2.GetDataSourceRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        data_source = grpc_call(grpc_handler.GetDataSource, req)

        result = data_source

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "dataSource", name, project, allow_cache
            )
            result["relationships"] = relationships

        if result:
            spec = result.get("spec", result)
            name = spec.get("name", result.get("name", "default_source"))
            source_type = spec.get("type", "").upper()
            if source_type == "REQUEST_SOURCE":
                schema_fields = []
                feast_types = set()
                for field in spec.get("requestDataOptions", {}).get("schema", []):
                    value_type_enum = _convert_value_type_str_to_value_type(
                        field.get("valueType", "").upper()
                    )
                    feast_type = from_value_type(value_type_enum)
                    dtype = getattr(feast_type, "__name__", str(feast_type))
                    schema_fields.append(
                        f'        Field(name="{field["name"]}", dtype={dtype}),'
                    )
                    feast_types.add(dtype)
                context = dict(
                    name=name,
                    schema_lines=schema_fields,
                    feast_types=list(feast_types),
                )
                result["featureDefinition"] = render_request_source_code(context)
                return result
            elif source_type == "PUSH_SOURCE" and "batchSource" in spec:
                batch_source_name = spec["batchSource"].get("name", "batch_source")
                context = dict(
                    name=name,
                    batch_source_name=batch_source_name,
                )
                result["featureDefinition"] = render_push_source_code(context)
                return result
            else:
                path = spec.get("path") or spec.get("fileOptions", {}).get("uri", "")
                timestamp_field = spec.get("timestampField", "event_timestamp")
                created_timestamp_column = spec.get("createdTimestampColumn", "")
                context = dict(
                    name=name,
                    path=path,
                    timestamp_field=timestamp_field,
                    created_timestamp_column=created_timestamp_column,
                )
                result["featureDefinition"] = render_data_source_code(context)
                return result

    return router
