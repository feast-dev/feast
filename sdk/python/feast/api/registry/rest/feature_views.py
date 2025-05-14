import logging
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, Query, Body

from feast.api.registry.rest.rest_utils import grpc_call, parse_tags
from feast.registry_server import RegistryServer_pb2
from feast.feature_view import FeatureView
from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config
from feast.data_source import DataSource
from feast.entity import Entity

logger = logging.getLogger(__name__)


def get_feature_view_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_views/{name}")
    def get_any_feature_view(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetAnyFeatureView, req)
        return response.get("anyFeatureView", {})

    @router.get("/feature_views")
    def list_all_feature_views(
        project: str = Query(...),
        allow_cache: bool = Query(True),
        tags: Dict[str, str] = Depends(parse_tags),
    ):
        req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        return grpc_call(grpc_handler.ListAllFeatureViews, req)
    
    @router.post("/feature-views")
    def create_feature_view(
        feature_view_data: Dict[str, Any] = Body(...),
    ):
        try:
            name = feature_view_data.get("name")
            description = feature_view_data.get("description", "")
            tags = feature_view_data.get("tags", {})
            owner = feature_view_data.get("owner", "")
            project = feature_view_data.get("project", "default")
            ttl = feature_view_data.get("ttl")
            
            entity_names = feature_view_data.get("entities", [])
            
            data_source_name = feature_view_data.get("dataSource")
            
            features_data = feature_view_data.get("features", [])
            
            repo_config = load_repo_config()
            fs = FeatureStore(config=repo_config)
            
            entities = []
            for entity_name in entity_names:
                entity = fs.get_entity(entity_name, project)
                if entity:
                    entities.append(entity)
                else:
                    raise ValueError(f"Entity {entity_name} not found in project {project}")
            
            data_source = fs.get_data_source(data_source_name, project)
            if not data_source:
                raise ValueError(f"Data source {data_source_name} not found in project {project}")
            
            features = []
            for feature_data in features_data:
                feature_name = feature_data.get("name")
                feature_type = feature_data.get("valueType")
                features.append(
                    FeatureView.Feature(
                        name=feature_name,
                        dtype=feature_type,
                    )
                )
            
            feature_view = FeatureView(
                name=name,
                entities=entities,
                features=features,
                ttl=ttl,
                source=data_source,
                tags=tags,
                owner=owner,
                description=description,
            )
            
            fs.apply(feature_view)
            
            return {"status": "success", "message": f"Feature view {name} created successfully"}
        except Exception as e:
            logger.exception(f"Error creating feature view: {str(e)}")
            return {"status": "error", "detail": str(e)}

    return router
