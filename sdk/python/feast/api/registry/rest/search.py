import logging
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from feast.api.registry.rest.rest_utils import (
    get_all_project_resources,
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
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        """
        Search across all Feast resources including:
        - Entities
        - Feature Views
        - Features
        - Feature Services
        - Data Sources
        - Saved Datasets
        Project Selection:
        - No projects parameter: Search all projects (default)
        - projects=["proj1"]: Search single project
        - projects=["proj1", "proj2"]: Search multiple projects
        Sorting:
        - Supports sorting by match_score, name, or type
        - Can specify sort_order as asc or desc
        """

        # Validate sorting parameters
        sort_by = sorting_params.get("sort_by", "")
        sort_order = sorting_params.get("sort_order", "")

        # Validate sort_by parameter
        valid_sort_fields = ["match_score", "name", "type"]
        if sort_by and sort_by not in valid_sort_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid sort_by parameter: '{sort_by}'. Valid options are: {valid_sort_fields}",
            )

        # Validate sort_order parameter (this should already be validated by Query regex, but double-check)
        valid_sort_orders = ["asc", "desc"]
        if sort_order and sort_order not in valid_sort_orders:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid sort_order parameter: '{sort_order}'. Valid options are: {valid_sort_orders}",
            )

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

            # if requested project/s doesn't exist, return empty results
            if len(existing_projects) == 0:
                response = {
                    "results": [],
                    "total_count": 0,
                    "query": query,
                    "projects_searched": [],
                    "error": "No projects found",
                }
                return response

            # search only existing ones
            projects_to_search = existing_projects
        else:
            # No specific projects - search all projects
            projects_to_search = list(available_projects)

        # Search across all specified projects using helper function
        for current_project in projects_to_search:
            try:
                # Get all resources for this project
                project_resources = get_all_project_resources(
                    grpc_handler,
                    current_project,
                    allow_cache,
                    tags,
                    None,
                    sorting_params,
                )

                # Extract and convert entities
                entities = project_resources.get("entities", [])
                for entity in entities:
                    results.append(
                        {
                            "type": "entity",
                            "name": entity.get("spec", {}).get("name", ""),
                            "description": entity.get("spec", {}).get(
                                "description", ""
                            ),
                            "tags": entity.get("spec", {}).get("tags", {}),
                            "data": entity,
                            "project": current_project,
                        }
                    )

                # Extract and convert data sources
                data_sources = project_resources.get("dataSources", [])
                for ds in data_sources:
                    results.append(
                        {
                            "type": "dataSource",
                            "name": ds.get("dataSource", {}).get("name", "")
                            or ds.get("name", ""),
                            "description": ds.get("dataSource", {}).get(
                                "description", ""
                            )
                            or ds.get("description", ""),
                            "tags": ds.get("dataSource", {}).get("tags", {})
                            or ds.get("tags", {}),
                            "data": ds,
                            "project": current_project,
                        }
                    )

                # Extract and convert feature views
                feature_views = project_resources.get("featureViews", [])
                for fv in feature_views:
                    results.append(
                        {
                            "type": "featureView",
                            "name": fv.get("featureView", {})
                            .get("spec", {})
                            .get("name", ""),
                            "description": fv.get("featureView", {})
                            .get("spec", {})
                            .get("description", ""),
                            "tags": fv.get("featureView", {})
                            .get("spec", {})
                            .get("tags", {}),
                            "data": fv,
                            "project": current_project,
                        }
                    )

                # Extract and convert features
                features = project_resources.get("features", [])
                for feature in features:
                    results.append(
                        {
                            "type": "feature",
                            "name": feature.get("name", ""),
                            "description": feature.get("description", ""),
                            "tags": feature.get("tags", {}),
                            "data": feature,
                            "project": current_project,
                        }
                    )

                # Extract and convert feature services
                feature_services = project_resources.get("featureServices", [])
                for fs in feature_services:
                    results.append(
                        {
                            "type": "featureService",
                            "name": fs.get("featureService", {})
                            .get("spec", {})
                            .get("name", "")
                            or fs.get("spec", {}).get("name", ""),
                            "description": fs.get("featureService", {})
                            .get("spec", {})
                            .get("description", "")
                            or fs.get("spec", {}).get("description", ""),
                            "tags": fs.get("featureService", {})
                            .get("spec", {})
                            .get("tags", {})
                            or fs.get("spec", {}).get("tags", {}),
                            "data": fs,
                            "project": current_project,
                        }
                    )

                # Extract and convert saved datasets
                saved_datasets = project_resources.get("savedDatasets", [])
                for sd in saved_datasets:
                    results.append(
                        {
                            "type": "savedDataset",
                            "name": sd.get("savedDataset", {})
                            .get("spec", {})
                            .get("name", "")
                            or sd.get("spec", {}).get("name", ""),
                            "description": sd.get("savedDataset", {})
                            .get("spec", {})
                            .get("description", "")
                            or sd.get("spec", {}).get("description", ""),
                            "tags": sd.get("savedDataset", {})
                            .get("spec", {})
                            .get("tags", {})
                            or sd.get("spec", {}).get("tags", {}),
                            "data": sd,
                            "project": current_project,
                        }
                    )

            except Exception as e:
                logger.error(
                    f"Error getting resources for project '{current_project}': {e}"
                )
                continue

        # Apply search filtering
        filtered_results = _filter_search_results(results, query)

        # Apply sorting
        sorted_results = _sort_search_results(filtered_results, sorting_params)

        response = {
            "results": sorted_results,
            "total_count": len(filtered_results),
            "query": query,
            "projects_searched": projects_to_search,
        }

        return response

    return router


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

    return sorted(results, key=lambda x: x.get(sort_by, ""), reverse=reverse)


def _fuzzy_match(query: str, text: str, threshold: float = 0.6) -> bool:
    """Simple fuzzy matching using character overlap"""
    if not query or not text:
        return False

    query_chars = set(query)
    text_chars = set(text)

    overlap = len(query_chars.intersection(text_chars))
    similarity = overlap / len(query_chars.union(text_chars))

    return similarity >= threshold
