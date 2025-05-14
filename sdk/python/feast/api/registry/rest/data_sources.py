import logging
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, Query, Body

from feast.api.registry.rest.rest_utils import grpc_call, parse_tags
from feast.protos.feast.registry import RegistryServer_pb2
from feast.data_source import (
    DataSource,
    FileSource,
    BigQuerySource,
    KafkaSource,
    KinesisSource,
    PushSource,
)
from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config

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
    
    @router.post("/data-sources")
    def create_data_source(
        data_source_data: Dict[str, Any] = Body(...),
    ):
        try:
            name = data_source_data.get("name")
            description = data_source_data.get("description", "")
            tags = data_source_data.get("tags", {})
            owner = data_source_data.get("owner", "")
            project = data_source_data.get("project", "default")
            source_type = data_source_data.get("type")
            
            data_source = None
            
            if source_type == "file":
                file_options = data_source_data.get("file_options", {})
                data_source = FileSource(
                    name=name,
                    path=file_options.get("path"),
                    timestamp_field=file_options.get("timestamp_field"),
                    created_timestamp_column=file_options.get("created_timestamp_column"),
                    event_timestamp_column=file_options.get("event_timestamp_column"),
                    description=description,
                    tags=tags,
                    owner=owner,
                )
            elif source_type == "bigquery":
                bq_options = data_source_data.get("bigquery_options", {})
                data_source = BigQuerySource(
                    name=name,
                    table=bq_options.get("table"),
                    timestamp_field=bq_options.get("timestamp_field"),
                    created_timestamp_column=bq_options.get("created_timestamp_column"),
                    event_timestamp_column=bq_options.get("event_timestamp_column"),
                    description=description,
                    tags=tags,
                    owner=owner,
                )
            elif source_type == "kafka":
                kafka_options = data_source_data.get("kafka_options", {})
                data_source = KafkaSource(
                    name=name,
                    bootstrap_servers=kafka_options.get("bootstrap_servers"),
                    topic=kafka_options.get("topic"),
                    timestamp_field=kafka_options.get("timestamp_field"),
                    description=description,
                    tags=tags,
                    owner=owner,
                )
            elif source_type == "kinesis":
                kinesis_options = data_source_data.get("kinesis_options", {})
                data_source = KinesisSource(
                    name=name,
                    region=kinesis_options.get("region"),
                    stream_name=kinesis_options.get("stream_name"),
                    timestamp_field=kinesis_options.get("timestamp_field"),
                    description=description,
                    tags=tags,
                    owner=owner,
                )
            elif source_type == "push":
                push_options = data_source_data.get("push_options", {})
                data_source = PushSource(
                    name=name,
                    timestamp_field=push_options.get("timestamp_field"),
                    description=description,
                    tags=tags,
                    owner=owner,
                )
            else:
                raise ValueError(f"Unsupported data source type: {source_type}")
            
            repo_config = load_repo_config()
            fs = FeatureStore(config=repo_config)
            fs.apply(data_source)
            
            return {"status": "success", "message": f"Data source {name} created successfully"}
        except Exception as e:
            logger.exception(f"Error creating data source: {str(e)}")
            return {"status": "error", "detail": str(e)}

    return router
