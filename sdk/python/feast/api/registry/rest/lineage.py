"""REST API endpoints for registry lineage and relationships."""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_all_project_resources,
    get_pagination_params,
    get_sorting_params,
    grpc_call,
)
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


def get_lineage_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/lineage/registry")
    def get_registry_lineage(
        project: str = Query(...),
        allow_cache: bool = Query(True),
        filter_object_type: Optional[str] = Query(None),
        filter_object_name: Optional[str] = Query(None),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        """
        Get complete registry lineage with relationships and indirect relationships.
        Args:
            project: Project name
            allow_cache: Whether to allow cached data
            filter_object_type: Optional filter by object type (dataSource, entity, featureView, featureService, feature)
            filter_object_name: Optional filter by object name
        Returns:
            Dictionary containing relationships and indirect_relationships arrays
        """
        req = RegistryServer_pb2.GetRegistryLineageRequest(
            project=project,
            allow_cache=allow_cache,
            filter_object_type=filter_object_type or "",
            filter_object_name=filter_object_name or "",
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )

        response = grpc_call(grpc_handler.GetRegistryLineage, req)
        return {
            "relationships": response.get("relationships", []),
            "indirect_relationships": response.get("indirectRelationships", []),
            "relationships_pagination": response.get("relationshipsPagination", {}),
            "indirect_relationships_pagination": response.get(
                "indirectRelationshipsPagination", {}
            ),
        }

    @router.get("/lineage/objects/{object_type}/{object_name}")
    def get_object_relationships_path(
        object_type: str,
        object_name: str,
        project: str = Query(...),
        include_indirect: bool = Query(False),
        allow_cache: bool = Query(True),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        """
        Get relationships for a specific object.
        Args:
            object_type: Type of object (dataSource, entity, featureView, featureService, feature)
            object_name: Name of the object
            project: Project name
            include_indirect: Whether to include indirect relationships
            allow_cache: Whether to allow cached data
        Returns:
            Dictionary containing relationships array for the specific object
        """
        valid_types = [
            "dataSource",
            "entity",
            "featureView",
            "featureService",
            "feature",
        ]
        if object_type not in valid_types:
            raise ValueError(
                f"Invalid object_type. Must be one of: {', '.join(valid_types)}"
            )

        req = RegistryServer_pb2.GetObjectRelationshipsRequest(
            project=project,
            object_type=object_type,
            object_name=object_name,
            include_indirect=include_indirect,
            allow_cache=allow_cache,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )

        return grpc_call(grpc_handler.GetObjectRelationships, req)

    @router.get("/lineage/complete")
    def get_complete_registry_data(
        project: str = Query(...),
        allow_cache: bool = Query(True),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        """
        Get complete registry data.
        This endpoint provides all the data the UI currently loads:
        - All registry objects
        - Relationships
        - Indirect relationships
        - Merged feature view data
        - Features

        Args:
            project: Project name
            allow_cache: Whether to allow cached data
            pagination_params: Pagination parameters (page, page_size)
            sorting_params: Sorting parameters (sort_by, sort_order)

        Returns:
            Complete registry data structure with pagination metadata.

        Note:
            Pagination and sorting are applied to each object type separately.
        """
        # Create pagination and sorting parameters for gRPC calls
        grpc_pagination = create_grpc_pagination_params(pagination_params)
        grpc_sorting = create_grpc_sorting_params(sorting_params)

        # Get lineage data
        lineage_req = RegistryServer_pb2.GetRegistryLineageRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
        )
        lineage_response = grpc_call(grpc_handler.GetRegistryLineage, lineage_req)

        # Get all registry objects using shared helper function
        project_resources, pagination, errors = get_all_project_resources(
            grpc_handler,
            project,
            allow_cache,
            tags={},
            pagination_params=pagination_params,
            sorting_params=sorting_params,
        )
        if errors and not project_resources:
            logger.error(
                f"Error getting project resources for project {project}: {errors}"
            )
            return {
                "project": project,
                "objects": {},
                "relationships": [],
                "indirectRelationships": [],
                "pagination": {},
            }
        return {
            "project": project,
            "objects": {
                "entities": project_resources.get("entities", []),
                "dataSources": project_resources.get("dataSources", []),
                "featureViews": project_resources.get("featureViews", []),
                "featureServices": project_resources.get("featureServices", []),
                "features": project_resources.get("features", []),
            },
            "relationships": lineage_response.get("relationships", []),
            "indirectRelationships": lineage_response.get("indirectRelationships", []),
            "pagination": {
                # Get pagination metadata from project_resources if available, otherwise use empty dicts
                "entities": pagination.get("entities", {}),
                "dataSources": pagination.get("dataSources", {}),
                "featureViews": pagination.get("featureViews", {}),
                "featureServices": pagination.get("featureServices", {}),
                "features": pagination.get("features", {}),
                "relationships": lineage_response.get("relationshipsPagination", {}),
                "indirectRelationships": lineage_response.get(
                    "indirectRelationshipsPagination", {}
                ),
            },
        }

    @router.get("/lineage/registry/all")
    def get_registry_lineage_all(
        allow_cache: bool = Query(True),
        filter_object_type: Optional[str] = Query(None),
        filter_object_name: Optional[str] = Query(None),
    ):
        projects_resp = grpc_call(
            grpc_handler.ListProjects,
            RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
        )
        projects = projects_resp.get("projects", [])
        all_relationships = []
        all_indirect_relationships = []
        for project in projects:
            project_name = project["spec"]["name"]
            req = RegistryServer_pb2.GetRegistryLineageRequest(
                project=project_name,
                allow_cache=allow_cache,
                filter_object_type=filter_object_type or "",
                filter_object_name=filter_object_name or "",
            )
            response = grpc_call(grpc_handler.GetRegistryLineage, req)
            relationships = response.get("relationships", [])
            indirect_relationships = response.get("indirectRelationships", [])
            # Optionally add project info to each relationship
            for rel in relationships:
                rel["project"] = project_name
            for rel in indirect_relationships:
                rel["project"] = project_name
            all_relationships.extend(relationships)
            all_indirect_relationships.extend(indirect_relationships)
        return {
            "relationships": all_relationships,
            "indirect_relationships": all_indirect_relationships,
        }

    @router.get("/lineage/complete/all")
    def get_complete_registry_data_all(
        allow_cache: bool = Query(True),
    ):
        projects_resp = grpc_call(
            grpc_handler.ListProjects,
            RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
        )
        projects = projects_resp.get("projects", [])
        all_data = []
        for project in projects:
            project_name = project["spec"]["name"]
            # Get lineage data
            lineage_req = RegistryServer_pb2.GetRegistryLineageRequest(
                project=project_name,
                allow_cache=allow_cache,
            )
            lineage_response = grpc_call(grpc_handler.GetRegistryLineage, lineage_req)

            # Get all registry objects using shared helper function
            project_resources, _, errors = get_all_project_resources(
                grpc_handler, project_name, allow_cache, tags={}
            )

            if errors and not project_resources:
                logger.error(
                    f"Error getting project resources for project {project_name}: {errors}"
                )
                continue

            # Add project field to each object
            for entity in project_resources.get("entities", []):
                entity["project"] = project_name
            for ds in project_resources.get("dataSources", []):
                ds["project"] = project_name
            for fv in project_resources.get("featureViews", []):
                fv["project"] = project_name
            for fs in project_resources.get("featureServices", []):
                fs["project"] = project_name
            for feat in project_resources.get("features", []):
                feat["project"] = project_name
            all_data.append(
                {
                    "project": project_name,
                    "objects": {
                        "entities": project_resources.get("entities", []),
                        "dataSources": project_resources.get("dataSources", []),
                        "featureViews": project_resources.get("featureViews", []),
                        "featureServices": project_resources.get("featureServices", []),
                        "features": project_resources.get("features", []),
                    },
                    "relationships": lineage_response.get("relationships", []),
                    "indirectRelationships": lineage_response.get(
                        "indirectRelationships", []
                    ),
                }
            )
        return {"projects": all_data}

    return router
