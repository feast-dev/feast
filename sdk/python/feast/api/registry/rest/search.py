import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    filter_search_results_and_match_score,
    get_all_project_resources,
    list_all_projects,
    paginate_and_sort,
    parse_tags,
    validate_or_set_default_pagination_params,
    validate_or_set_default_sorting_params,
)

logger = logging.getLogger(__name__)

custom_sorting = validate_or_set_default_sorting_params(
    sort_by_options=["match_score", "name", "type"],
    default_sort_by_option="match_score",
    default_sort_order="desc",
)

custom_pagination = validate_or_set_default_pagination_params(
    default_page=1,
    default_limit=50,
)


def get_search_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/search")
    def search_resources(
        query: str = Query(..., description="Search query string"),
        projects: Optional[List[str]] = Query(
            default=[],
            description="Project names to search in (optional - searches all projects if not specified)",
        ),
        allow_cache: bool = Query(default=True),
        tags: Dict[str, str] = Depends(parse_tags),
        sorting_params: dict = Depends(custom_sorting),
        pagination_params: dict = Depends(custom_pagination),
    ) -> Dict[str, Any]:
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
        results = []
        errors = []

        # Get list of all available projects for validation
        err_msg = ""

        projects_to_search, err_msg = _validate_projects(
            projects, grpc_handler, allow_cache
        )

        if err_msg:
            errors.append(err_msg)

        if not projects_to_search:
            return {
                "query": query,
                "projects_searched": projects_to_search,
                "results": [],
                "pagination": {},
                "errors": errors,
            }

        # Search across all specified projects using helper function
        for current_project in projects_to_search:
            try:
                # Get all resources for this project
                project_resources, _, resource_errors = get_all_project_resources(
                    grpc_handler,
                    current_project,
                    allow_cache,
                    tags,
                    None,
                    sorting_params,
                )
                errors.extend(resource_errors)

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
                            "project": current_project,
                            "tags": entity.get("spec", {}).get("tags", {}),
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
                            "project": current_project,
                            "tags": ds.get("dataSource", {}).get("tags", {})
                            or ds.get("tags", {}),
                        }
                    )

                # Extract and convert feature views (all types - future-proof)
                feature_views = project_resources.get("featureViews", [])
                for fv in feature_views:
                    # Find the feature view data by looking for keys that contain "feature" and "view"
                    feature_view_data = None
                    for key, value in fv.items():
                        if (
                            isinstance(value, dict)
                            and "feature" in key.lower()
                            and "view" in key.lower()
                        ):
                            feature_view_data = value
                            break

                    if feature_view_data:
                        results.append(
                            {
                                "type": "featureView",
                                "name": feature_view_data.get("spec", {}).get(
                                    "name", ""
                                ),
                                "description": feature_view_data.get("spec", {}).get(
                                    "description", ""
                                ),
                                "project": current_project,
                                "tags": feature_view_data.get("spec", {}).get(
                                    "tags", {}
                                ),
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
                            "project": current_project,
                            "featureView": feature.get("featureView", ""),
                            "tags": feature.get("tags", {}),
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
                            "project": current_project,
                            "tags": fs.get("featureService", {})
                            .get("spec", {})
                            .get("tags", {})
                            or fs.get("spec", {}).get("tags", {}),
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
                            "project": current_project,
                            "tags": sd.get("savedDataset", {})
                            .get("spec", {})
                            .get("tags", {})
                            or sd.get("spec", {}).get("tags", {}),
                        }
                    )

            except Exception as e:
                err_msg = f"Error getting resources for project '{current_project}'"
                logger.error(f"{err_msg}: {e}")
                errors.append(err_msg)
                continue

        # Apply search filtering
        filtered_results = filter_search_results_and_match_score(results, query)

        # Paginate & sort results
        paginated_results, pagination = paginate_and_sort(
            items=filtered_results,
            page=pagination_params["page"],
            limit=pagination_params["limit"],
            sort_by=sorting_params["sort_by"],
            sort_order=sorting_params["sort_order"],
        )

        # Remove tags from results before returning to user
        cleaned_result = _remove_tags_from_results(paginated_results)

        response = {
            "query": query,
            "projects_searched": projects_to_search,
            "results": cleaned_result,
            "pagination": pagination,
            "errors": errors,
        }

        return response

    return router


def _validate_projects(
    input_projects: Optional[List[str]], grpc_handler, allow_cache: bool
) -> tuple[List[str], str]:
    """Validate projects and return list of existing projects"""
    projects_to_search = []
    nonexistent_projects = []
    err_msg = ""

    # Handling case of empty projects parameter i.e. /search?query=user&projects=
    if input_projects is None:
        input_projects = []
    input_projects = [p for p in input_projects if p and p.strip()]

    try:
        all_projects, _, err_msg = list_all_projects(
            grpc_handler=grpc_handler,
            allow_cache=allow_cache,
        )

        if all_projects == []:
            err_msg = "No projects found"
        else:
            project_names = {
                proj.get("spec", {}).get("name", "")
                for proj in all_projects
                if proj.get("spec", {}).get("name")
            }

            if input_projects:
                for project in input_projects:
                    if project in project_names:
                        projects_to_search.append(project)
                    else:
                        nonexistent_projects.append(project)
            else:
                projects_to_search = list(project_names)

            if nonexistent_projects:
                err_msg = f"Following projects do not exist: {', '.join(nonexistent_projects)}"
                logger.error(f"{err_msg}")

    except Exception as e:
        err_msg = "Error getting projects"
        logger.error(f"{err_msg}: {e}")

    finally:
        return list(set(projects_to_search)), err_msg


def _remove_tags_from_results(results: List[Dict]) -> List[Dict]:
    """Remove tags field from search results before returning to user"""
    cleaned_results = []
    for result in results:
        # Create a copy without the tags field
        cleaned_result = {k: v for k, v in result.items() if k != "tags"}
        cleaned_results.append(cleaned_result)
    return cleaned_results
