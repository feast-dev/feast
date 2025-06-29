"""REST API endpoints for registry lineage and relationships."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.registry import RegistryServer_pb2


def get_lineage_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/lineage/registry")
    def get_registry_lineage(
        project: str = Query(...),
        allow_cache: bool = Query(True),
        filter_object_type: Optional[str] = Query(None),
        filter_object_name: Optional[str] = Query(None),
    ):
        """
        Get complete registry lineage with relationships and indirect relationships.
        Args:
            project: Project name
            allow_cache: Whether to allow cached data
            filter_object_type: Optional filter by object type (dataSource, entity, featureView, featureService)
            filter_object_name: Optional filter by object name
        Returns:
            Dictionary containing relationships and indirect_relationships arrays
        """
        req = RegistryServer_pb2.GetRegistryLineageRequest(
            project=project,
            allow_cache=allow_cache,
            filter_object_type=filter_object_type or "",
            filter_object_name=filter_object_name or "",
        )
        response = grpc_call(grpc_handler.GetRegistryLineage, req)

        return {
            "relationships": response.get("relationships", []),
            "indirect_relationships": response.get("indirectRelationships", []),
        }

    @router.get("/lineage/objects/{object_type}/{object_name}")
    def get_object_relationships(
        object_type: str,
        object_name: str,
        project: str = Query(...),
        include_indirect: bool = Query(False),
        allow_cache: bool = Query(True),
    ):
        """
        Get relationships for a specific object.
        Args:
            object_type: Type of object (dataSource, entity, featureView, featureService)
            object_name: Name of the object
            project: Project name
            include_indirect: Whether to include indirect relationships
            allow_cache: Whether to allow cached data
        Returns:
            Dictionary containing relationships array for the specific object
        """
        valid_types = ["dataSource", "entity", "featureView", "featureService"]
        if object_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid object_type. Must be one of: {valid_types}",
            )

        req = RegistryServer_pb2.GetObjectRelationshipsRequest(
            project=project,
            object_type=object_type,
            object_name=object_name,
            include_indirect=include_indirect,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetObjectRelationships, req)

        return {"relationships": response.get("relationships", [])}

    @router.get("/lineage/complete")
    def get_complete_registry_data(
        project: str = Query(...),
        allow_cache: bool = Query(True),
    ):
        """
        Get complete registry data.
        This endpoint provides all the data the UI currently loads:
        - All registry objects
        - Relationships
        - Indirect relationships
        - Merged feature view data
        Returns:
            Complete registry data structure.
        """
        # Get lineage data
        lineage_req = RegistryServer_pb2.GetRegistryLineageRequest(
            project=project,
            allow_cache=allow_cache,
        )
        lineage_response = grpc_call(grpc_handler.GetRegistryLineage, lineage_req)

        # Get all registry objects
        entities_req = RegistryServer_pb2.ListEntitiesRequest(
            project=project, allow_cache=allow_cache
        )
        entities_response = grpc_call(grpc_handler.ListEntities, entities_req)

        data_sources_req = RegistryServer_pb2.ListDataSourcesRequest(
            project=project, allow_cache=allow_cache
        )
        data_sources_response = grpc_call(
            grpc_handler.ListDataSources, data_sources_req
        )

        feature_views_req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project, allow_cache=allow_cache
        )
        feature_views_response = grpc_call(
            grpc_handler.ListAllFeatureViews, feature_views_req
        )

        feature_services_req = RegistryServer_pb2.ListFeatureServicesRequest(
            project=project, allow_cache=allow_cache
        )
        feature_services_response = grpc_call(
            grpc_handler.ListFeatureServices, feature_services_req
        )

        return {
            "project": project,
            "objects": {
                "entities": entities_response.get("entities", []),
                "dataSources": data_sources_response.get("dataSources", []),
                "featureViews": feature_views_response.get("featureViews", []),
                "featureServices": feature_services_response.get("featureServices", []),
            },
            "relationships": lineage_response.get("relationships", []),
            "indirectRelationships": lineage_response.get("indirectRelationships", []),
        }

    return router
