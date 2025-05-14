import logging
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, Query, Body

from feast.api.registry.rest.rest_utils import grpc_call, parse_tags
from feast.protos.feast.registry import RegistryServer_pb2
from feast.feature_service import FeatureService
from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config

logger = logging.getLogger(__name__)


def get_feature_service_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_services")
    def list_feature_services(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
    ):
        req = RegistryServer_pb2.ListFeatureServicesRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        return grpc_call(grpc_handler.ListFeatureServices, req)

    @router.get("/feature_services/{name}")
    def get_feature_service(
        name: str,
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
    ):
        req = RegistryServer_pb2.GetFeatureServiceRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        return grpc_call(grpc_handler.GetFeatureService, req)
    
    @router.post("/feature-services")
    def create_feature_service(
        feature_service_data: Dict[str, Any] = Body(...),
    ):
        try:
            name = feature_service_data.get("name")
            description = feature_service_data.get("description", "")
            tags = feature_service_data.get("tags", {})
            owner = feature_service_data.get("owner", "")
            project = feature_service_data.get("project", "default")
            
            feature_references_data = feature_service_data.get("feature_references", [])
            
            repo_config = load_repo_config()
            fs = FeatureStore(config=repo_config)
            
            feature_references = []
            for ref_data in feature_references_data:
                feature_view_name = ref_data.get("feature_view_name")
                feature_names = ref_data.get("feature_names", [])
                
                try:
                    fs.get_feature_view(feature_view_name, project)
                except Exception as e:
                    raise ValueError(f"Feature view {feature_view_name} not found in project {project}: {str(e)}")
                
                feature_references.append(
                    FeatureService.FeatureReference(
                        feature_view=feature_view_name,
                        features=feature_names,
                    )
                )
            
            feature_service = FeatureService(
                name=name,
                features=feature_references,
                tags=tags,
                owner=owner,
                description=description,
            )
            
            fs.apply(feature_service)
            
            return {"status": "success", "message": f"Feature service {name} created successfully"}
        except Exception as e:
            logger.exception(f"Error creating feature service: {str(e)}")
            return {"status": "error", "detail": str(e)}

    return router
