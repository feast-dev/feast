import logging
from typing import Any, Callable, Dict, List, Optional

from fastapi import HTTPException, Query
from google.protobuf.json_format import MessageToDict

from feast.errors import FeastObjectNotFoundException
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


def grpc_call(handler_fn, request):
    """
    Wrapper to invoke gRPC method with context=None and handle common errors.
    """
    try:
        response = handler_fn(request, context=None)
        return MessageToDict(response)
    except FeastObjectNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")


def get_object_relationships(
    grpc_handler,
    object_type: str,
    object_name: str,
    project: str,
    allow_cache: bool = True,
) -> List:
    """
    Get relationships for a specific object.

    Args:
        grpc_handler: The gRPC handler to use for calls
        object_type: Type of object (dataSource, entity, featureView, featureService)
        object_name: Name of the object
        project: Project name
        allow_cache: Whether to allow cached data

    Returns:
        List containing relationships for the object (both direct and indirect)
    """
    try:
        req = RegistryServer_pb2.GetObjectRelationshipsRequest(
            project=project,
            object_type=object_type,
            object_name=object_name,
            include_indirect=True,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetObjectRelationships, req)
        return response.get("relationships", [])
    except Exception:
        # If relationships can't be retrieved, return empty list rather than failing
        return []


def get_relationships_for_objects(
    grpc_handler,
    objects: List[Dict],
    object_type: str,
    project: str,
    allow_cache: bool = True,
) -> Dict[str, List]:
    """
    Get relationships for multiple objects efficiently.

    Args:
        grpc_handler: The gRPC handler to use for calls
        objects: List of objects to get relationships for
        object_type: Type of objects (dataSource, entity, featureView, featureService, feature)
        project: Project name
        allow_cache: Whether to allow cached data

    Returns:
        Dictionary mapping object names to their relationships (both direct and indirect)
    """
    relationships_map = {}

    for obj in objects:
        obj_name = None
        if object_type == "feature":
            obj_name = obj.get("name")
        elif isinstance(obj, dict):
            obj_name = (
                obj.get("name")
                or obj.get("spec", {}).get("name")
                or obj.get("meta", {}).get("name")
            )

        if obj_name:
            rels = get_object_relationships(
                grpc_handler,
                object_type,
                obj_name,
                project,
                allow_cache,
            )
            relationships_map[obj_name] = rels

    return relationships_map


def aggregate_across_projects(
    grpc_handler,
    list_method: Callable,
    request_cls: Callable,
    response_key: str,
    object_type: str,
    allow_cache: bool = True,
    page: int = 1,
    limit: int = 50,
    sort_by: Optional[str] = None,
    sort_order: str = "asc",
    include_relationships: bool = False,
) -> Dict:
    """
    Fetches and aggregates objects across all projects, adds project field, handles relationships, and paginates/sorts.
    """
    projects_resp = grpc_call(
        grpc_handler.ListProjects,
        RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
    )
    projects = projects_resp.get("projects", [])
    all_objects = []
    relationships_map = {}

    for project in projects:
        project_name = project["spec"]["name"]
        req = request_cls(
            project=project_name,
            allow_cache=allow_cache,
        )
        response = grpc_call(list_method, req)
        objects = response.get(response_key, [])
        for obj in objects:
            obj["project"] = project_name
        all_objects.extend(objects)
        if include_relationships:
            rels = get_relationships_for_objects(
                grpc_handler, objects, object_type, project_name, allow_cache
            )
            relationships_map.update(rels)

    paged_objects, pagination = paginate_and_sort(
        all_objects, page, limit, sort_by, sort_order
    )
    result = {
        response_key: paged_objects,
        "pagination": pagination,
    }
    if include_relationships:
        result["relationships"] = relationships_map
    return result


def parse_tags(tags: List[str] = Query(default=[])) -> Dict[str, str]:
    """
    Parses query strings like ?tags=key1:value1&tags=key2:value2 into a dict.
    """
    parsed_tags = {}
    for tag in tags:
        if ":" not in tag:
            continue
        key, value = tag.split(":", 1)
        parsed_tags[key] = value
    return parsed_tags


def get_pagination_params(
    page: Optional[int] = Query(None, ge=1),
    limit: Optional[int] = Query(None, ge=1, le=100),
) -> dict:
    return {
        "page": page or 0,
        "limit": limit or 0,
    }


def get_sorting_params(
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query(None, pattern="^(asc|desc)$"),
) -> dict:
    return {
        "sort_by": sort_by or "",
        "sort_order": sort_order or "asc",
    }


def validate_or_set_default_sorting_params(
    sort_by_options: List[str] = [],
    default_sort_by_option: str = "",
    default_sort_order: str = "asc",
) -> Callable:
    """
    Factory function to create a FastAPI dependency for validating sorting parameters.

    Args:
        sort_by_options: List of valid sort_by field names. If empty, no validation is performed
        default_sort_by_option: Default sort_by value if not provided
        default_sort_order: Default sort_order value if not provided (asc/desc)

    Returns:
        Callable that can be used as FastAPI dependency for sorting validation

    Example usage:
        # Create a custom sorting validator for specific fields
        custom_sorting = validate_or_set_default_sorting_params(
            sort_by_options=["name", "created_at", "updated_at"],
            default_sort_by_option="name",
            default_sort_order="asc"
        )

        # Use in FastAPI route
        @router.get("/items")
        def get_items(sorting_params: dict = Depends(custom_sorting)):
            sort_by = sorting_params["sort_by"]
            sort_order = sorting_params["sort_order"]
            # Use sort_by and sort_order for your logic
    """

    def set_input_or_default(
        sort_by: Optional[str] = Query(None), sort_order: Optional[str] = Query(None)
    ) -> dict:
        sorting_params = {}

        # If no sort options are configured, return defaults without validation
        if not sort_by_options:
            return {"sort_by": default_sort_by_option, "sort_order": default_sort_order}

        # Validate and set sort_by parameter
        if sort_by:
            if sort_by in sort_by_options:
                sorting_params["sort_by"] = sort_by
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid sort_by parameter: '{sort_by}'. Valid options are: {sort_by_options}",
                )
        else:
            # Use default if not provided
            sorting_params["sort_by"] = default_sort_by_option

        # Validate and set sort_order parameter
        if sort_order:
            if sort_order in ["asc", "desc"]:
                sorting_params["sort_order"] = sort_order
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid sort_order parameter: '{sort_order}'. Valid options are: ['asc', 'desc']",
                )
        else:
            # Use default if not provided
            sorting_params["sort_order"] = default_sort_order

        return sorting_params

    return set_input_or_default


def validate_or_set_default_pagination_params(
    default_page: int = 1,
    default_limit: int = 50,
    min_page: int = 1,
    min_limit: int = 1,
    max_limit: int = 100,
) -> Callable:
    """
    Factory function to create a FastAPI dependency for validating pagination parameters.

    Args:
        default_page: Default page number if not provided
        default_limit: Default limit if not provided
        min_page: Minimum allowed page number
        min_limit: Minimum allowed limit
        max_limit: Maximum allowed limit

    Returns:
        Callable that can be used as FastAPI dependency for pagination validation

    Example usage:
        # Create a custom pagination validator
        custom_pagination = validate_or_set_default_pagination_params(
            default_page=1,
            default_limit=25,
            max_limit=200
        )

        # Use in FastAPI route
        @router.get("/items")
        def get_items(pagination_params: dict = Depends(custom_pagination)):
            page = pagination_params["page"]
            limit = pagination_params["limit"]
            # Use page and limit for your logic
    """

    def set_input_or_default(
        page: Optional[int] = Query(None), limit: Optional[int] = Query(None)
    ) -> dict:
        pagination_params = {}

        # Validate and set page parameter
        if page is not None:
            if page < min_page:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid page parameter: '{page}'. Must be greater than or equal to {min_page}",
                )
            pagination_params["page"] = page
        else:
            pagination_params["page"] = default_page

        # Validate and set limit parameter
        if limit is not None:
            if limit < min_limit:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid limit parameter: '{limit}'. Must be greater than or equal to {min_limit}",
                )
            if limit > max_limit:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid limit parameter: '{limit}'. Must be less than or equal to {max_limit}",
                )
            pagination_params["limit"] = limit
        else:
            pagination_params["limit"] = default_limit

        return pagination_params

    return set_input_or_default


def create_grpc_pagination_params(
    pagination_params: dict,
) -> RegistryServer_pb2.PaginationParams:
    return RegistryServer_pb2.PaginationParams(
        page=pagination_params.get("page", 0),
        limit=pagination_params.get("limit", 0),
    )


def create_grpc_sorting_params(
    sorting_params: dict,
) -> RegistryServer_pb2.SortingParams:
    return RegistryServer_pb2.SortingParams(
        sort_by=sorting_params.get("sort_by", ""),
        sort_order=sorting_params.get("sort_order", "asc"),
    )


def paginate_and_sort(
    items: list,
    page: int,
    limit: int,
    sort_by: Optional[str] = None,
    sort_order: str = "asc",
):
    if sort_by:
        items = sorted(
            items, key=lambda x: x.get(sort_by, ""), reverse=(sort_order == "desc")
        )
    total = len(items)
    start = (page - 1) * limit
    end = start + limit
    paged_items = items[start:end]
    pagination = {}
    if page:
        pagination["page"] = page
    if limit:
        pagination["limit"] = limit
    if total:
        pagination["totalCount"] = total
    total_pages = (total + limit - 1) // limit
    if total_pages:
        pagination["totalPages"] = total_pages
    if end < total:
        pagination["hasNext"] = True
    if start > 0:
        pagination["hasPrevious"] = True
    return paged_items, pagination


def get_all_project_resources(
    grpc_handler,
    project: str,
    allow_cache: bool,
    tags: Dict[str, str],
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> Dict[str, Any]:
    """
    Helper function to get all resources for a project with optional sorting and pagination
    Returns a dictionary with resource types as keys and lists of resources as values
    Also includes pagination metadata when pagination_params are provided
    """
    # Create grpc pagination and sorting parameters if provided
    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    resources: Dict[str, Any] = {
        "entities": [],
        "dataSources": [],
        "featureViews": [],
        "featureServices": [],
        "savedDatasets": [],
        "features": [],
    }

    try:
        # Get entities
        entities_req = RegistryServer_pb2.ListEntitiesRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
        )
        entities_response = grpc_call(grpc_handler.ListEntities, entities_req)
        resources["entities"] = entities_response.get("entities", [])

        # Get data sources
        data_sources_req = RegistryServer_pb2.ListDataSourcesRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
            tags=tags,
        )
        data_sources_response = grpc_call(
            grpc_handler.ListDataSources, data_sources_req
        )
        resources["dataSources"] = data_sources_response.get("dataSources", [])

        # Get feature views
        feature_views_req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
            tags=tags,
        )
        feature_views_response = grpc_call(
            grpc_handler.ListAllFeatureViews, feature_views_req
        )
        resources["featureViews"] = feature_views_response.get("featureViews", [])

        # Get feature services
        feature_services_req = RegistryServer_pb2.ListFeatureServicesRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
            tags=tags,
        )
        feature_services_response = grpc_call(
            grpc_handler.ListFeatureServices, feature_services_req
        )
        resources["featureServices"] = feature_services_response.get(
            "featureServices", []
        )

        # Get saved datasets
        saved_datasets_req = RegistryServer_pb2.ListSavedDatasetsRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
            tags=tags,
        )
        saved_datasets_response = grpc_call(
            grpc_handler.ListSavedDatasets, saved_datasets_req
        )
        resources["savedDatasets"] = saved_datasets_response.get("savedDatasets", [])

        # Get features
        features_req = RegistryServer_pb2.ListFeaturesRequest(
            project=project,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
        )
        features_response = grpc_call(grpc_handler.ListFeatures, features_req)
        resources["features"] = features_response.get("features", [])

        # Include pagination metadata if pagination was requested
        if pagination_params:
            resources["pagination"] = {
                "entities": entities_response.get("pagination", {}),
                "dataSources": data_sources_response.get("pagination", {}),
                "featureViews": feature_views_response.get("pagination", {}),
                "featureServices": feature_services_response.get("pagination", {}),
                "savedDatasets": saved_datasets_response.get("pagination", {}),
                "features": features_response.get("pagination", {}),
            }

        return resources

    except Exception as e:
        logger.error(f"Error getting resources for project '{project}': {e}")
        return resources  # Return empty resources dict on error


def filter_search_results_and_match_score(
    results: List[Dict], query: str
) -> List[Dict]:
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
        if fuzzy_match(query_lower, result.get("name", "").lower()):
            result["match_score"] = 40
            filtered_results.append(result)

    return filtered_results


def fuzzy_match(query: str, text: str, threshold: float = 0.6) -> bool:
    """Simple fuzzy matching using character overlap"""
    if not query or not text:
        return False

    query_chars = set(query)
    text_chars = set(text)

    overlap = len(query_chars.intersection(text_chars))
    similarity = overlap / len(query_chars.union(text_chars))

    return similarity >= threshold
