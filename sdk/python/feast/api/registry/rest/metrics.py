import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from feast.api.registry.rest.rest_utils import (
    get_pagination_params,
    get_sorting_params,
    grpc_call,
    paginate_and_sort,
)
from feast.protos.feast.registry import RegistryServer_pb2


def get_metrics_router(grpc_handler, server=None) -> APIRouter:
    logger = logging.getLogger(__name__)
    router = APIRouter()

    @router.get("/metrics/resource_counts", tags=["Metrics"])
    async def resource_counts(
        project: Optional[str] = Query(
            None, description="Project name to filter resource counts"
        ),
    ):
        def count_resources_for_project(project_name: str):
            entities = grpc_call(
                grpc_handler.ListEntities,
                RegistryServer_pb2.ListEntitiesRequest(project=project_name),
            )
            data_sources = grpc_call(
                grpc_handler.ListDataSources,
                RegistryServer_pb2.ListDataSourcesRequest(project=project_name),
            )
            try:
                saved_datasets = grpc_call(
                    grpc_handler.ListSavedDatasets,
                    RegistryServer_pb2.ListSavedDatasetsRequest(project=project_name),
                )
            except Exception:
                saved_datasets = {"savedDatasets": []}
            try:
                features = grpc_call(
                    grpc_handler.ListFeatures,
                    RegistryServer_pb2.ListFeaturesRequest(project=project_name),
                )
            except Exception:
                features = {"features": []}
            try:
                feature_views = grpc_call(
                    grpc_handler.ListFeatureViews,
                    RegistryServer_pb2.ListFeatureViewsRequest(project=project_name),
                )
            except Exception:
                feature_views = {"featureViews": []}
            try:
                feature_services = grpc_call(
                    grpc_handler.ListFeatureServices,
                    RegistryServer_pb2.ListFeatureServicesRequest(project=project_name),
                )
            except Exception:
                feature_services = {"featureServices": []}
            return {
                "entities": len(entities.get("entities", [])),
                "dataSources": len(data_sources.get("dataSources", [])),
                "savedDatasets": len(saved_datasets.get("savedDatasets", [])),
                "features": len(features.get("features", [])),
                "featureViews": len(feature_views.get("featureViews", [])),
                "featureServices": len(feature_services.get("featureServices", [])),
            }

        if project:
            counts = count_resources_for_project(project)
            return {"project": project, "counts": counts}
        else:
            # List all projects via gRPC
            projects_resp = grpc_call(
                grpc_handler.ListProjects, RegistryServer_pb2.ListProjectsRequest()
            )
            all_projects = [
                p["spec"]["name"] for p in projects_resp.get("projects", [])
            ]
            all_counts = {}
            total_counts = {
                "entities": 0,
                "dataSources": 0,
                "savedDatasets": 0,
                "features": 0,
                "featureViews": 0,
                "featureServices": 0,
            }
            for project_name in all_projects:
                counts = count_resources_for_project(project_name)
                all_counts[project_name] = counts
                for k in total_counts:
                    total_counts[k] += counts[k]
            return {"total": total_counts, "perProject": all_counts}

    @router.get("/metrics/recently_visited", tags=["Metrics"])
    async def recently_visited(
        request: Request,
        project: Optional[str] = Query(
            None, description="Project name to filter recent visits"
        ),
        object_type: Optional[str] = Query(
            None,
            alias="object",
            description="Object type to filter recent visits (e.g., entities, features)",
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        user = None
        if hasattr(request.state, "user"):
            user = getattr(request.state, "user", None)
        if not user:
            user = "anonymous"
        project_val = project or (server.store.project if server else None)
        key = f"recently_visited_{user}"
        logger.info(
            f"[/metrics/recently_visited] Project: {project_val}, Key: {key}, Object: {object_type}"
        )
        try:
            visits_json = (
                server.registry.get_project_metadata(project_val, key)
                if server
                else None
            )
            visits = json.loads(visits_json) if visits_json else []
        except Exception:
            visits = []
        if object_type:
            visits = [v for v in visits if v.get("object") == object_type]

        server_limit = getattr(server, "recent_visits_limit", 100) if server else 100
        visits = visits[-server_limit:]

        page = pagination_params.get("page", 0)
        limit = pagination_params.get("limit", 0)
        sort_by = sorting_params.get("sort_by")
        sort_order = sorting_params.get("sort_order", "asc")

        if page == 0 and limit == 0:
            if sort_by:
                visits = sorted(
                    visits,
                    key=lambda x: x.get(sort_by, ""),
                    reverse=(sort_order == "desc"),
                )
            return {"visits": visits, "pagination": {"totalCount": len(visits)}}
        else:
            if page == 0:
                page = 1
            if limit == 0:
                limit = 50
            paged_visits, pagination = paginate_and_sort(
                visits, page, limit, sort_by, sort_order
            )
            return {
                "visits": paged_visits,
                "pagination": pagination,
            }

    return router
