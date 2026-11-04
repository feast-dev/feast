from fastapi import APIRouter, Depends, Query

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


def get_label_view_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get("/label_views")
    def list_label_views(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        include_relationships: bool = Query(
            False, description="Include relationships for each label view"
        ),
        tags: dict = Depends(parse_tags),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        req = RegistryServer_pb2.ListLabelViewsRequest(
            project=project,
            allow_cache=allow_cache,
            tags=tags,
            pagination=create_grpc_pagination_params(pagination_params),
            sorting=create_grpc_sorting_params(sorting_params),
        )
        response = grpc_call(grpc_handler.ListLabelViews, req)
        label_views = response.get("labelViews", [])

        for lv in label_views:
            lv["type"] = "labelView"

        result = {
            "featureViews": label_views,
            "pagination": response.get("pagination", {}),
        }

        if include_relationships:
            relationships = get_relationships_for_objects(
                grpc_handler, label_views, "featureView", project, allow_cache
            )
            result["relationships"] = relationships

        return result

    @router.get("/label_views/all")
    def list_label_views_all(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        sort_by: str = Query(None),
        sort_order: str = Query("asc"),
        include_relationships: bool = Query(
            False, description="Include relationships for each label view"
        ),
    ):
        projects_resp = grpc_call(
            grpc_handler.ListProjects,
            RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache),
        )
        projects = projects_resp.get("projects", [])
        all_label_views = []
        for project in projects:
            project_name = project["spec"]["name"]
            req = RegistryServer_pb2.ListLabelViewsRequest(
                project=project_name,
                allow_cache=allow_cache,
            )
            response = grpc_call(grpc_handler.ListLabelViews, req)
            label_views = response.get("labelViews", [])
            for lv in label_views:
                lv["type"] = "labelView"
                lv["project"] = project_name
            all_label_views.extend(label_views)

        paged_label_views, pagination = paginate_and_sort(
            all_label_views, page, limit, sort_by, sort_order
        )
        result = {
            "featureViews": paged_label_views,
            "pagination": pagination,
        }
        if include_relationships:
            relationships_map = {}
            for project in projects:
                project_name = project["spec"]["name"]
                project_lvs = [
                    lv for lv in all_label_views if lv.get("project") == project_name
                ]
                rels = get_relationships_for_objects(
                    grpc_handler,
                    project_lvs,
                    "featureView",
                    project_name,
                    allow_cache,
                )
                relationships_map.update(rels)
            result["relationships"] = relationships_map
        return result

    @router.get("/label_views/{name}")
    def get_label_view(
        name: str,
        project: str = Query(...),
        include_relationships: bool = Query(
            False, description="Include relationships for this label view"
        ),
        allow_cache: bool = Query(True),
    ):
        req = RegistryServer_pb2.GetLabelViewRequest(
            name=name,
            project=project,
            allow_cache=allow_cache,
        )
        response = grpc_call(grpc_handler.GetLabelView, req)
        response["type"] = "labelView"

        if include_relationships:
            relationships = get_object_relationships(
                grpc_handler, "featureView", name, project, allow_cache
            )
            response["relationships"] = relationships

        return response

    return router
