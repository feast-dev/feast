import logging
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from feast.api.registry.rest.rest_utils import (
    get_sorting_params,
    grpc_call,
    parse_tags,
)
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


def get_search_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/search")
    def search_resources(
        query: str = Query(..., description="Search query string"),
        projects: Optional[List[str]] = Query(
            default=None,
            description="Project names to search in (optional - searches all projects if not specified)",
        ),
        resource_types: List[str] = Query(
            default=[],
            description="Filter by resource types: entities, feature_views, feature_services, data_sources, saved_datasets, permissions, projects",
        ),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        """
        Search across all Feast resources including:
        - Entities
        - Feature Views
        - Feature Services
        - Data Sources
        - Saved Datasets
        - Permissions
        - Projects
        Project Selection:
        - No projects parameter: Search all projects (default)
        - projects=["proj1"]: Search single project
        - projects=["proj1", "proj2"]: Search multiple projects
        """

        # Validate sorting parameters
        sort_by = sorting_params.get("sort_by", "")
        sort_order = sorting_params.get("sort_order", "")

        # Validate sort_by parameter
        valid_sort_fields = ["match_score", "name", "type"]
        if sort_by and sort_by not in valid_sort_fields:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid sort_by parameter: '{sort_by}'. Valid options are: {valid_sort_fields}",
            )

        # Validate sort_order parameter (this should already be validated by Query regex, but double-check)
        valid_sort_orders = ["asc", "desc"]
        if sort_order and sort_order not in valid_sort_orders:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid sort_order parameter: '{sort_order}'. Valid options are: {valid_sort_orders}",
            )

        # Validate resource types
        valid_resource_types = {
            "entities",
            "feature_views",
            "feature_services",
            "data_sources",
            "saved_datasets",
            "permissions",
            "projects",
        }

        if resource_types:
            valid_types = [rt for rt in resource_types if rt in valid_resource_types]
            invalid_types = [
                rt for rt in resource_types if rt not in valid_resource_types
            ]

            if invalid_types:
                # Log warnings for invalid resource types
                logger.warning(
                    f"The following resource types are invalid and will be ignored: {invalid_types}"
                )

                if len(valid_types) == 0:
                    # Single invalid resource type - don't search at all
                    return {
                        "results": [],
                        "total_count": 0,
                        "query": query,
                        "resource_types": [],  # Don't include invalid types in response
                        "projects_searched": [],
                    }

            # Use only valid resource types for the search
            resource_types = valid_types

        # Default to all resource types if none specified
        if not resource_types:
            resource_types = [
                "entities",
                "feature_views",
                "feature_services",
                "data_sources",
                "saved_datasets",
                "permissions",
                "projects",
            ]

        results = []

        # Get list of all available projects for validation
        try:
            projects_req = RegistryServer_pb2.ListProjectsRequest(
                allow_cache=allow_cache,
                tags=tags,
            )
            projects_response = grpc_call(grpc_handler.ListProjects, projects_req)
            all_projects = projects_response.get("projects", [])
            available_projects = {
                proj.get("spec", {}).get("name", "")
                for proj in all_projects
                if proj.get("spec", {}).get("name")
            }
        except Exception as e:
            logger.error(f"Error getting projects: {e}")
            available_projects = set()

        # Get list of projects to search in
        # Handle when projects parameter is not provided (None) or empty strings
        if projects is None:
            # No projects parameter provided - search all projects
            filtered_projects = []
        else:
            # Handle empty string in projects list (from URL like "projects=")
            filtered_projects = [p for p in projects if p and p.strip()]

        if filtered_projects:
            # Specific projects requested - validate they exist
            existing_projects = []
            nonexistent_projects = []

            for project in filtered_projects:
                if project in available_projects:
                    existing_projects.append(project)
                else:
                    nonexistent_projects.append(project)

            # Log warnings for non-existent projects
            if nonexistent_projects:
                logger.warning(
                    f"The following projects do not exist and will be ignored: {nonexistent_projects}"
                )

            # Handle single project case - if only one project requested and it doesn't exist
            if len(filtered_projects) == 1 and len(existing_projects) == 0:
                # Single non-existent project - don't search at all
                return {
                    "results": [],
                    "total_count": 0,
                    "query": query,
                    "resource_types": resource_types,
                    "projects_searched": [],
                }

            # Multiple projects case - search only existing ones
            projects_to_search = existing_projects
        else:
            # No specific projects - search all projects
            projects_to_search = list(available_projects)

        # Search across all specified projects
        for current_project in projects_to_search:
            # Search entities
            if "entities" in resource_types:
                entities = _search_entities(
                    grpc_handler, query, current_project, allow_cache, tags
                )
                results.extend(entities)

            # Search feature views
            if "feature_views" in resource_types:
                feature_views = _search_feature_views(
                    grpc_handler, query, current_project, allow_cache, tags
                )
                results.extend(feature_views)

            # Search feature services
            if "feature_services" in resource_types:
                feature_services = _search_feature_services(
                    grpc_handler, query, current_project, allow_cache, tags
                )
                results.extend(feature_services)

            # Search data sources
            if "data_sources" in resource_types:
                data_sources = _search_data_sources(
                    grpc_handler, query, current_project, allow_cache, tags
                )
                results.extend(data_sources)

            # Search saved datasets
            if "saved_datasets" in resource_types:
                saved_datasets = _search_saved_datasets(
                    grpc_handler, query, current_project, allow_cache, tags
                )
                results.extend(saved_datasets)

            # Search permissions
            if "permissions" in resource_types:
                permissions = _search_permissions(
                    grpc_handler, query, current_project, allow_cache, tags
                )
                results.extend(permissions)

        # Search projects (filter by projects_to_search if specified)
        if "projects" in resource_types:
            all_projects_resources = _search_projects(
                grpc_handler, query, allow_cache, tags
            )

            # Filter projects based on projects_to_search if specific projects were requested
            if filtered_projects:  # If specific projects were requested
                filtered_projects_resources = [
                    proj
                    for proj in all_projects_resources
                    if proj.get("name", "") in projects_to_search
                ]
                results.extend(filtered_projects_resources)
            else:
                # No specific projects - include all projects
                results.extend(all_projects_resources)

        # Apply search filtering
        filtered_results = _filter_search_results(results, query)

        # Apply sorting
        sorted_results = _sort_search_results(filtered_results, sorting_params)

        return {
            "results": sorted_results,
            "total_count": len(filtered_results),
            "query": query,
            "resource_types": resource_types,
            "projects_searched": projects_to_search,
        }

    return router


def _search_entities(
    grpc_handler, query: str, project: str, allow_cache: bool, tags: Dict[str, str]
) -> List[Dict]:
    """Search entities"""
    try:
        req = RegistryServer_pb2.ListEntitiesRequest(
            project=project,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.ListEntities, req)
        entities = response.get("entities", [])

        return [
            {
                "type": "entity",
                "name": entity.get("spec", {}).get("name", ""),
                "description": entity.get("spec", {}).get("description", ""),
                "tags": entity.get("spec", {}).get("tags", {}),
                "data": entity,
                "project": project,
            }
            for entity in entities
        ]
    except Exception as e:
        logger.error(f"Error searching entities in project '{project}': {e}")
        return []


def _search_feature_views(
    grpc_handler, query: str, project: str, allow_cache: bool, tags: Dict[str, str]
) -> List[Dict]:
    """Search feature views"""
    try:
        req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListAllFeatureViews, req)
        feature_views = response.get("featureViews", [])

        return [
            {
                "type": "feature_view",
                "name": fv.get("featureView", {}).get("spec", {}).get("name", ""),
                "description": fv.get("featureView", {})
                .get("spec", {})
                .get("description", ""),
                "tags": fv.get("featureView", {}).get("spec", {}).get("tags", {}),
                "features": [
                    f.get("name", "")
                    for f in fv.get("featureView", {})
                    .get("spec", {})
                    .get("features", [])
                ],
                "data": fv,
                "project": project,
            }
            for fv in feature_views
        ]
    except Exception as e:
        logger.error(f"Error searching feature views: {e}")
        return []


def _search_feature_services(
    grpc_handler, query: str, project: str, allow_cache: bool, tags: Dict[str, str]
) -> List[Dict]:
    """Search feature services"""
    try:
        req = RegistryServer_pb2.ListFeatureServicesRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListFeatureServices, req)
        feature_services = response.get("featureServices", [])

        return [
            {
                "type": "feature_service",
                "name": fs.get("featureService", {}).get("spec", {}).get("name", "")
                or fs.get("spec", {}).get("name", ""),
                "description": fs.get("featureService", {})
                .get("spec", {})
                .get("description", "")
                or fs.get("spec", {}).get("description", ""),
                "tags": fs.get("featureService", {}).get("spec", {}).get("tags", {})
                or fs.get("spec", {}).get("tags", {}),
                "features": [
                    f.get("name", "")
                    for f in (
                        fs.get("featureService", {}).get("spec", {}).get("features", [])
                        or fs.get("spec", {}).get("features", [])
                    )
                ],
                "data": fs,
                "project": project,
            }
            for fs in feature_services
        ]
    except Exception as e:
        logger.error(f"Error searching feature services: {e}")
        return []


def _search_data_sources(
    grpc_handler, query: str, project: str, allow_cache: bool, tags: Dict[str, str]
) -> List[Dict]:
    """Search data sources"""
    try:
        req = RegistryServer_pb2.ListDataSourcesRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListDataSources, req)
        data_sources = response.get("dataSources", [])

        return [
            {
                "type": "data_source",
                "name": ds.get("dataSource", {}).get("name", "") or ds.get("name", ""),
                "description": ds.get("dataSource", {}).get("description", "")
                or ds.get("description", ""),
                "tags": ds.get("dataSource", {}).get("tags", {}) or ds.get("tags", {}),
                "data": ds,
                "project": project,
            }
            for ds in data_sources
        ]
    except Exception as e:
        logger.error(f"Error searching data sources: {e}")
        return []


def _search_saved_datasets(
    grpc_handler, query: str, project: str, allow_cache: bool, tags: Dict[str, str]
) -> List[Dict]:
    """Search saved datasets"""
    try:
        req = RegistryServer_pb2.ListSavedDatasetsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListSavedDatasets, req)
        saved_datasets = response.get("savedDatasets", [])

        return [
            {
                "type": "saved_dataset",
                "name": sd.get("savedDataset", {}).get("spec", {}).get("name", "")
                or sd.get("spec", {}).get("name", ""),
                "description": sd.get("savedDataset", {})
                .get("spec", {})
                .get("description", "")
                or sd.get("spec", {}).get("description", ""),
                "tags": sd.get("savedDataset", {}).get("spec", {}).get("tags", {})
                or sd.get("spec", {}).get("tags", {}),
                "data": sd,
                "project": project,
            }
            for sd in saved_datasets
        ]
    except Exception as e:
        logger.error(f"Error searching saved datasets: {e}")
        return []


def _search_permissions(
    grpc_handler, query: str, project: str, allow_cache: bool, tags: Dict[str, str]
) -> List[Dict]:
    """Search permissions"""
    try:
        req = RegistryServer_pb2.ListPermissionsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListPermissions, req)
        permissions = response.get("permissions", [])

        return [
            {
                "type": "permission",
                "name": perm.get("permission", {}).get("spec", {}).get("name", "")
                or perm.get("spec", {}).get("name", ""),
                "description": perm.get("permission", {})
                .get("spec", {})
                .get("description", "")
                or perm.get("spec", {}).get("description", ""),
                "tags": perm.get("permission", {}).get("spec", {}).get("tags", {})
                or perm.get("spec", {}).get("tags", {}),
                "data": perm,
                "project": project,
            }
            for perm in permissions
        ]
    except Exception as e:
        logger.error(f"Error searching permissions: {e}")
        return []


def _search_projects(
    grpc_handler, query: str, allow_cache: bool, tags: Dict[str, str]
) -> List[Dict]:
    """Search projects"""
    try:
        req = RegistryServer_pb2.ListProjectsRequest(
            allow_cache=allow_cache,
            tags=tags,
        )
        response = grpc_call(grpc_handler.ListProjects, req)
        projects = response.get("projects", [])

        return [
            {
                "type": "project",
                "name": proj.get("spec", {}).get("name", ""),
                "description": proj.get("spec", {}).get("description", ""),
                "tags": proj.get("spec", {}).get("tags", {}),
                "data": proj,
                "project": proj.get("spec", {}).get("name", ""),
            }
            for proj in projects
        ]
    except Exception as e:
        logger.error(f"Error searching projects: {e}")
        return []


def _filter_search_results(results: List[Dict], query: str) -> List[Dict]:
    """Filter search results based on query string"""
    if not query:
        return results

    query_lower = query.lower()
    filtered_results = []

    for result in results:
        # Search in name
        if query_lower in result.get("name", "").lower():
            result["match_score"] = 100  # Exact name match gets highest score
            filtered_results.append(result)
            continue

        # Search in description
        if query_lower in result.get("description", "").lower():
            result["match_score"] = 80
            filtered_results.append(result)
            continue

        # Search in tags
        tags = result.get("tags", {})
        tag_match = False
        for key, value in tags.items():
            if query_lower in key.lower() or query_lower in str(value).lower():
                tag_match = True
                break

        if tag_match:
            result["match_score"] = 60
            filtered_results.append(result)
            continue

        # Search in features (for feature views and services)
        features = result.get("features", [])
        feature_match = any(query_lower in feature.lower() for feature in features)

        if feature_match:
            result["match_score"] = 70
            filtered_results.append(result)
            continue

        # Partial name match (fuzzy search)
        if _fuzzy_match(query_lower, result.get("name", "").lower()):
            result["match_score"] = 40
            filtered_results.append(result)

    return filtered_results


def _sort_search_results(results: List[Dict], sorting_params: dict) -> List[Dict]:
    """Sort search results"""
    sort_by = sorting_params.get("sort_by", "match_score")
    sort_order = sorting_params.get("sort_order", "desc")

    reverse = sort_order == "desc"

    if sort_by == "match_score":
        return sorted(results, key=lambda x: x.get("match_score", 0), reverse=reverse)
    elif sort_by == "name":
        return sorted(results, key=lambda x: x.get("name", ""), reverse=reverse)
    elif sort_by == "type":
        return sorted(results, key=lambda x: x.get("type", ""), reverse=reverse)

    return results


def _fuzzy_match(query: str, text: str, threshold: float = 0.6) -> bool:
    """Simple fuzzy matching using character overlap"""
    if not query or not text:
        return False

    query_chars = set(query)
    text_chars = set(text)

    overlap = len(query_chars.intersection(text_chars))
    similarity = overlap / len(query_chars.union(text_chars))

    return similarity >= threshold
