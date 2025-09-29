from typing import Dict

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.codegen_utils import render_feature_view_code
from feast.api.registry.rest.rest_utils import (
    create_grpc_pagination_params,
    create_grpc_sorting_params,
    get_object_relationships,
    get_pagination_params,
    get_relationships_for_objects,
    get_sorting_params,
    grpc_call,
    paginate_and_sort,
    parse_tags,
)
from feast.registry_server import RegistryServer_pb2
from feast.type_map import _convert_value_type_str_to_value_type
from feast.types import from_value_type


def _extract_feature_view_from_any(any_feature_view: dict) -> dict:
    """Extract the specific feature view type and data from an AnyFeatureView object.

    Args:
        any_feature_view: Dictionary containing the AnyFeatureView data

    Returns:
        Dictionary with 'type' and feature view data, or empty dict if no valid type found
    """
    for key, value in any_feature_view.items():
        if value:
            return {"type": key, **value}

    return {}


def extract_feast_types_from_fields(fields):
    types = set()
    for field in fields:
        value_type_enum = _convert_value_type_str_to_value_type(
            field.get("valueType", "").upper()
        )
        feast_type = from_value_type(value_type_enum)
        dtype = (
            feast_type.__name__ if hasattr(feast_type, "__name__") else str(feast_type)
        )
        types.add(dtype)
    return list(types)


def get_feature_view_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/feature_views/all")
    def list_feature_views_all(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature view"
        ),
    ):
        projects_resp = grpc_call(
            grpc_handler.ListProjects,
            RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
        )
        projects = projects_resp.get("projects", [])
        all_feature_views = []
        for project in projects:
            project_name = project["spec"]["name"]
            req = RegistryServer_pb2.ListAllFeatureViewsRequest(
                project=project_name,
                allow_cache=allow_cache,
            )
            response = grpc_call(grpc_handler.ListAllFeatureViews, req)
            any_feature_views = response.get("featureViews", [])
            for any_feature_view in any_feature_views:
                feature_view = _extract_feature_view_from_any(any_feature_view)
                if feature_view:
                    feature_view["project"] = project_name
                    all_feature_views.append(feature_view)
        paged_feature_views, pagination = paginate_and_sort(
            all_feature_views, page, limit, sort_by, sort_order
        )
        result = {
            "featureViews": paged_feature_views,
            "pagination": pagination,
        }
        if include_relationships:
            relationships_map = {}
            for project in projects:
                project_name = project["spec"]["name"]
                # Filter feature views for this project
                project_feature_views = [
                    fv for fv in all_feature_views if fv["project"] == project_name
                ]
                rels = get_relationships_for_objects(
                    grpc_handler,
                    project_feature_views,
                    "featureView",
                    project_name,
                    allow_cache,
                )
                relationships_map.update(rels)
            result["relationships"] = relationships_map
        return result

    @router.get("/feature_views/{name}")
    def get_any_feature_view(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this feature view"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetAnyFeatureViewRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetAnyFeatureView, req)
        any_feature_view = response.get("anyFeatureView", {})

        result = _extract_feature_view_from_any(any_feature_view)

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "featureView", name, project, allow_cache
            )
            result["relationships"] = relationships

        if result and "spec" in result:
            spec = result["spec"]
            fv_type = result.get("type", "featureView")
            features = spec.get("features", [])
            if not isinstance(features, list):
                features = []
            if fv_type == "onDemandFeatureView":
                class_name = "OnDemandFeatureView"
                sources = spec.get("sources", {})
                if sources:
                    source_exprs = []
                    for k, v in sources.items():
                        var_name = v.get("name", k) if isinstance(v, dict) else str(v)
                        source_exprs.append(f'"{k}": {var_name}')
                    source_name = "{" + ", ".join(source_exprs) + "}"
                else:
                    source_name = "source_feature_view"
            elif fv_type == "streamFeatureView":
                class_name = "StreamFeatureView"
                stream_source = spec.get("streamSource", {})
                source_name = stream_source.get("name", "stream_source")
            else:
                class_name = "FeatureView"
                source_views = spec.get("source_views") or spec.get("sourceViews")
                if source_views and isinstance(source_views, list):
                    source_vars = [sv.get("name", "source_view") for sv in source_views]
                    source_name = "[" + ", ".join(source_vars) + "]"
                else:
                    source = spec.get("source")
                    if isinstance(source, dict) and source.get("name"):
                        source_name = source["name"]
                    else:
                        batch_source = spec.get("batchSource", {})
                        source_name = batch_source.get("name", "driver_stats_source")

            # Entities
            entities = spec.get("entities") or []
            entities_str = ", ".join(entities)

            # Feature schema
            schema_lines = []
            for field in features:
                value_type_enum = _convert_value_type_str_to_value_type(
                    field.get("valueType", "").upper()
                )
                feast_type = from_value_type(value_type_enum)
                dtype = getattr(feast_type, "__name__", str(feast_type))
                desc = field.get("description")
                desc_str = f', description="{desc}"' if desc else ""
                schema_lines.append(
                    f'        Field(name="{field["name"]}", dtype={dtype}{desc_str}),'
                )

            # Feast types
            feast_types = extract_feast_types_from_fields(features)

            # Tags
            tags = spec.get("tags", {})
            tags_str = f"tags={tags}," if tags else ""

            # TTL
            ttl = spec.get("ttl")
            ttl_str = "timedelta(days=1)"
            if ttl:
                if isinstance(ttl, int):
                    ttl_str = f"timedelta(seconds={ttl})"
                elif isinstance(ttl, str) and ttl.endswith("s") and ttl[:-1].isdigit():
                    ttl_str = f"timedelta(seconds={int(ttl[:-1])})"

            # Online
            online = spec.get("online", True)

            # Build context
            context = dict(
                class_name=class_name,
                name=spec.get("name", "example"),
                entities_str=entities_str,
                ttl_str=ttl_str,
                schema_lines=schema_lines,
                online=online,
                source_name=source_name,
                tags_str=tags_str,
                feast_types=feast_types,
            )

            result["featureDefinition"] = render_feature_view_code(context)

        return result

    @router.get("/feature_views")
    def list_all_feature_views(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        include_relationships: bool = Query(
            False, description="Include relationships for each feature view"
        ),
        entity: str = Query(None, description="Filter feature views by entity name"),
        feature: str = Query(None, description="Filter feature views by feature name"),
        feature_service: str = Query(
            None, description="Filter feature views by feature service name"
        ),
        data_source: str = Query(
            None, description="Filter feature views by data source name"
        ),
        tags: Dict[str, str] = Depends(parse_tags),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListAllFeatureViewsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            entity=entity,
            feature=feature,
            feature_service=feature_service,
            data_source=data_source,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListAllFeatureViews, req)
        any_feature_views = response.get("featureViews", [])

        # Extract the specific type of feature view from each AnyFeatureView
        feature_views = []
        for any_feature_view in any_feature_views:
            feature_view = _extract_feature_view_from_any(any_feature_view)
            if feature_view:
                feature_views.append(feature_view)

        result = {
            "featureViews": feature_views,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, feature_views, "featureView", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    return router
