import logging
from typing import Dict, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.registry import RegistryServer_pb2
from feast.type_map import _convert_value_type_str_to_value_type
from feast.types import from_value_type

logger = logging.getLogger(__name__)


class FileOptionsModel(BaseModel):
    uri: str = ""


class BigQueryOptionsModel(BaseModel):
    table: str = ""
    query: str = ""


class SnowflakeOptionsModel(BaseModel):
    table: str = ""
    database: str = ""
    schema_: str = ""


class RedshiftOptionsModel(BaseModel):
    table: str = ""
    database: str = ""
    schema_: str = ""


class KafkaOptionsModel(BaseModel):
    kafka_bootstrap_servers: str = ""
    topic: str = ""


class SparkOptionsModel(BaseModel):
    table: str = ""
    path: str = ""


class ApplyDataSourceRequestBody(BaseModel):
    name: str
    project: str
    type: Optional[int] = None
    timestamp_field: Optional[str] = ""
    created_timestamp_column: Optional[str] = ""
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = {}
    owner: Optional[str] = ""
    file_options: Optional[FileOptionsModel] = None
    bigquery_options: Optional[BigQueryOptionsModel] = None
    snowflake_options: Optional[SnowflakeOptionsModel] = None
    redshift_options: Optional[RedshiftOptionsModel] = None
    kafka_options: Optional[KafkaOptionsModel] = None
    spark_options: Optional[SparkOptionsModel] = None


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

    @router.post("/data_sources", status_code=201)
    def apply_data_source(body: ApplyDataSourceRequestBody):
        ds_proto = DataSourceProto(
            name=body.name,
            timestamp_field=body.timestamp_field or "",
            created_timestamp_column=body.created_timestamp_column or "",
            description=body.description or "",
            tags=body.tags or {},
            owner=body.owner or "",
        )
        if body.type is not None:
            ds_proto.type = body.type  # type: ignore[assignment]

        if body.file_options:
            ds_proto.file_options.uri = body.file_options.uri
        elif body.bigquery_options:
            ds_proto.bigquery_options.table = body.bigquery_options.table
            ds_proto.bigquery_options.query = body.bigquery_options.query
        elif body.snowflake_options:
            ds_proto.snowflake_options.table = body.snowflake_options.table
            ds_proto.snowflake_options.database = body.snowflake_options.database
            ds_proto.snowflake_options.schema = body.snowflake_options.schema_
        elif body.redshift_options:
            ds_proto.redshift_options.table = body.redshift_options.table
            ds_proto.redshift_options.database = body.redshift_options.database
            ds_proto.redshift_options.schema = body.redshift_options.schema_
        elif body.kafka_options:
            ds_proto.kafka_options.kafka_bootstrap_servers = (
                body.kafka_options.kafka_bootstrap_servers
            )
            ds_proto.kafka_options.topic = body.kafka_options.topic
        elif body.spark_options:
            ds_proto.spark_options.table = body.spark_options.table
            ds_proto.spark_options.path = body.spark_options.path

        req = RegistryServer_pb2.ApplyDataSourceRequest(
            data_source=ds_proto,
            project=body.project,
            commit=True,
        )
        grpc_call(grpc_handler.ApplyDataSource, req)

        return JSONResponse(
            status_code=201,
            content={
                "name": body.name,
                "project": body.project,
                "status": "applied",
            },
        )

    @router.delete("/data_sources/{name}")
    def delete_data_source(
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.DeleteDataSourceRequest(
            name=name,
            project=project,
            commit=True,
        )
        grpc_call(grpc_handler.DeleteDataSource, req)

        return {"name": name, "project": project, "status": "deleted"}

    return router
