import logging
from typing import Any, Callable, Dict, List, Optional

from fastapi import HTTPException, Query
from google.protobuf.json_format import MessageToDict

from feast.errors import (
    FeastObjectNotFoundException,
    FeastPermissionError,
    PushSourceNotFoundException,
)
from feast.protos.feast.registry import RegistryServer_pb2

logger = logging.getLogger(__name__)


MATCH_SCORE_DEFAULT_THRESHOLD = 0.75
MATCH_SCORE_NAME = 100
MATCH_SCORE_DESCRIPTION = 80
MATCH_SCORE_TAGS = 60
MATCH_SCORE_PARTIAL = 40


def grpc_call(handler_fn, request):
    """
    Wrapper to invoke gRPC method with context=None and handle common errors.
    """
    try:
        response = handler_fn(request, context=None)
        return MessageToDict(response)
    except (
        FeastObjectNotFoundException,
        FeastPermissionError,
        PushSourceNotFoundException,
    ):
        raise
    except Exception as e:
        raise e


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


def get_all_feature_views(feature_views_response: dict) -> list[dict]:
    """
    Get all feature views from a feature views response, regardless of type.
    This is future-proof and will handle any kind of feature view keys.
    """
    result = []
    for key, value in feature_views_response.items():
        if isinstance(value, list):
            result.extend(value)
        else:
            result.append(value)
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
            return {
                "sort_by": default_sort_by_option,
                "sort_order": sort_order if sort_order else default_sort_order,
            }

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
    tags: Optional[Dict[str, str]] = None,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """
    Helper function to get all resources for a project with optional sorting and pagination
    Returns a dictionary with resource types as keys and lists of resources as values
    Also includes pagination metadata when pagination_params are provided
    """

    resources: Dict[str, Any] = {
        "entities": [],
        "dataSources": [],
        "featureViews": [],
        "featureServices": [],
        "savedDatasets": [],
        "features": [],
        "pagination": {},
        "errors": [],
    }
    pagination: dict = {}
    errors = []

    try:
        # Get entities
        resources["entities"], pagination["entities"], err_msg = list_entities(
            grpc_handler=grpc_handler,
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            pagination_params=pagination_params,
            sorting_params=sorting_params,
        )
        if err_msg:
            errors.append(err_msg)

        # Get data sources
        resources["dataSources"], pagination["dataSources"], err_msg = (
            list_data_sources(
                grpc_handler=grpc_handler,
                project=project,
                allow_cache=allow_cache,
                tags=tags,
                pagination_params=pagination_params,
                sorting_params=sorting_params,
            )
        )
        if err_msg:
            errors.append(err_msg)

        # Get feature views
        resources["featureViews"], pagination["featureViews"], err_msg = (
            list_feature_views(
                grpc_handler=grpc_handler,
                project=project,
                allow_cache=allow_cache,
                tags=tags,
                pagination_params=pagination_params,
                sorting_params=sorting_params,
            )
        )
        if err_msg:
            errors.append(err_msg)

        # Get feature services
        resources["featureServices"], pagination["featureServices"], err_msg = (
            list_feature_services(
                grpc_handler=grpc_handler,
                project=project,
                allow_cache=allow_cache,
                tags=tags,
                pagination_params=pagination_params,
                sorting_params=sorting_params,
            )
        )
        if err_msg:
            errors.append(err_msg)

        # Get saved datasets
        resources["savedDatasets"], pagination["savedDatasets"], err_msg = (
            list_saved_datasets(
                grpc_handler=grpc_handler,
                project=project,
                allow_cache=allow_cache,
                tags=tags,
                pagination_params=pagination_params,
                sorting_params=sorting_params,
            )
        )
        if err_msg:
            errors.append(err_msg)

        # Get features
        resources["features"], pagination["features"], err_msg = list_features(
            grpc_handler=grpc_handler,
            project=project,
            allow_cache=allow_cache,
            pagination_params=pagination_params,
            sorting_params=sorting_params,
        )
        if err_msg:
            errors.append(err_msg)

    except Exception as e:
        err_msg = f"Error getting resources for project '{project}'"
        errors.append(err_msg)
        logger.error(f"{err_msg}: {e}")
    finally:
        return resources, pagination, errors


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
            result["match_score"] = MATCH_SCORE_NAME
            filtered_results.append(result)
            continue

        # Search in description
        if query_lower in result.get("description", "").lower():
            result["match_score"] = MATCH_SCORE_DESCRIPTION
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
            result["match_score"] = MATCH_SCORE_TAGS
            filtered_results.append(result)
            continue

        # Partial name match (fuzzy search)
        fuzzy_match_score = fuzzy_match(query_lower, result.get("name", "").lower())
        if fuzzy_match_score >= MATCH_SCORE_DEFAULT_THRESHOLD:
            result["match_score"] = fuzzy_match_score * 100
            filtered_results.append(result)

    return filtered_results


def fuzzy_match(query: str, text: str) -> float:
    """Simple fuzzy matching using character overlap"""
    if not query or not text:
        return 0.0

    query_chars = set(query)
    text_chars = set(text)

    overlap = len(query_chars.intersection(text_chars))
    similarity = overlap / len(query_chars.union(text_chars))

    return similarity


def list_entities(
    grpc_handler,
    project: str,
    allow_cache: bool,
    tags: Optional[Dict[str, str]] = None,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Search entities in a project with optional sorting and pagination
    """
    entities = []
    pagination = {}
    err_msg = ""

    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    try:
        entities_req = RegistryServer_pb2.ListEntitiesRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
            tags=tags,
        )
        entities_response = grpc_call(grpc_handler.ListEntities, entities_req)
        entities, pagination = (
            entities_response.get("entities", []),
            entities_response.get("pagination", {}),
        )
    except Exception as e:
        err_msg = f"Error searching entities in project '{project}'"
        logger.error(f"{err_msg}: {e}")
    finally:
        return entities, pagination, err_msg


def list_feature_views(
    grpc_handler,
    project: str,
    allow_cache: bool,
    tags: Optional[Dict[str, str]] = None,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Search feature views in a project with optional sorting and pagination
    """
    feature_views = []
    pagination = {}
    err_msg = ""

    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    try:
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
        all_feature_views = get_all_feature_views(feature_views_response)
        feature_views, pagination = (
            all_feature_views,
            feature_views_response.get("pagination", {}),
        )
    except Exception as e:
        err_msg = f"Error searching feature views in project '{project}'"
        logger.error(f"{err_msg}: {e}")
    finally:
        return feature_views, pagination, err_msg


def list_feature_services(
    grpc_handler,
    project: str,
    allow_cache: bool,
    tags: Optional[Dict[str, str]] = None,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Search feature services in a project with optional sorting and pagination
    """
    feature_services = []
    pagination = {}
    err_msg = ""

    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    try:
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
        feature_services, pagination = (
            feature_services_response.get("featureServices", []),
            feature_services_response.get("pagination", {}),
        )
    except Exception as e:
        err_msg = f"Error searching feature services in project '{project}'"
        logger.error(f"{err_msg}: {e}")
    finally:
        return feature_services, pagination, err_msg


def list_data_sources(
    grpc_handler,
    project: str,
    allow_cache: bool,
    tags: Optional[Dict[str, str]] = None,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Search data sources in a project with optional sorting and pagination
    """
    data_sources = []
    pagination = {}
    err_msg = ""

    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    try:
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
        data_sources, pagination = (
            data_sources_response.get("dataSources", []),
            data_sources_response.get("pagination", {}),
        )
    except Exception as e:
        err_msg = f"Error searching data sources in project '{project}'"
        logger.error(f"{err_msg}: {e}")
    finally:
        return data_sources, pagination, err_msg


def list_saved_datasets(
    grpc_handler,
    project: str,
    allow_cache: bool,
    tags: Optional[Dict[str, str]] = None,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Search saved datasets in a project with optional sorting and pagination
    """
    saved_datasets = []
    pagination = {}
    err_msg = ""

    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    try:
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
        saved_datasets, pagination = (
            saved_datasets_response.get("savedDatasets", []),
            saved_datasets_response.get("pagination", {}),
        )
    except Exception as e:
        err_msg = f"Error searching saved datasets in project '{project}'"
        logger.error(f"{err_msg}: {e}")
    finally:
        return saved_datasets, pagination, err_msg


def list_features(
    grpc_handler,
    project: str,
    allow_cache: bool,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Search features in a project with optional sorting and pagination
    """
    features = []
    pagination = {}
    err_msg = ""

    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    try:
        features_req = RegistryServer_pb2.ListFeaturesRequest(
            project=project,
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
        )
        features_response = grpc_call(grpc_handler.ListFeatures, features_req)
        features, pagination = (
            features_response.get("features", []),
            features_response.get("pagination", {}),
        )
    except Exception as e:
        err_msg = f"Error searching features in project '{project}'"
        logger.error(f"{err_msg}: {e}")
    finally:
        return features, pagination, err_msg


def list_all_projects(
    grpc_handler,
    allow_cache: bool,
    tags: Optional[Dict[str, str]] = None,
    pagination_params: Optional[dict] = None,
    sorting_params: Optional[dict] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], str]:
    """
    Search all projects with optional sorting and pagination
    """
    projects = []
    pagination = {}
    err_msg = ""

    grpc_pagination = None
    grpc_sorting = None

    if pagination_params:
        grpc_pagination = create_grpc_pagination_params(pagination_params)
    if sorting_params:
        grpc_sorting = create_grpc_sorting_params(sorting_params)

    try:
        projects_req = RegistryServer_pb2.ListProjectsRequest(
            allow_cache=allow_cache,
            pagination=grpc_pagination,
            sorting=grpc_sorting,
            tags=tags,
        )
        projects_response = grpc_call(grpc_handler.ListProjects, projects_req)
        projects, pagination = (
            projects_response.get("projects", []),
            projects_response.get("pagination", {}),
        )
    except Exception as e:
        err_msg = "Error searching all projects"
        logger.error(f"{err_msg}: {e}")
    finally:
        return projects, pagination, err_msg
