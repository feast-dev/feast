import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from feast.api.registry.rest.rest_utils import (
    get_pagination_params,
    get_sorting_params,
    grpc_call,
)
from feast.protos.feast.registry import RegistryServer_pb2
from feast.repo_config import BATCH_ENGINE_CLASS_FOR_TYPE

logger = logging.getLogger(__name__)


def _extract_engine_info(store) -> Dict[str, Any]:
    """Extract compute engine configuration from the FeatureStore."""
    config = getattr(store, "config", None)
    if config is None:
        return {
            "engineType": "local",
            "engineClass": "LocalComputeEngine",
            "config": {"type": "local"},
        }

    batch_engine = getattr(config, "batch_engine", None)
    if batch_engine is None:
        return {
            "engineType": "local",
            "engineClass": "LocalComputeEngine",
            "config": {"type": "local"},
        }

    if isinstance(batch_engine, dict):
        engine_type = batch_engine.get("type", "local")
        engine_config = {k: v for k, v in batch_engine.items() if v is not None}
    elif hasattr(batch_engine, "type"):
        engine_type = str(batch_engine.type)
        dump = getattr(batch_engine, "model_dump", None) or getattr(
            batch_engine, "dict", None
        )
        if dump:
            engine_config = {k: v for k, v in dump().items() if v is not None}
        else:
            engine_config = {"type": engine_type}
    else:
        engine_type = str(batch_engine)
        engine_config = {"type": engine_type}

    engine_class_path = BATCH_ENGINE_CLASS_FOR_TYPE.get(engine_type, engine_type)
    engine_class = (
        engine_class_path.rsplit(".", 1)[-1]
        if "." in engine_class_path
        else engine_class_path
    )

    return {
        "engineType": engine_type,
        "engineClass": engine_class,
        "config": engine_config,
    }


def _extract_materialization_jobs(
    grpc_handler, project: str, allow_cache: bool = True
) -> List[Dict[str, Any]]:
    """Extract materialization job info from feature view metadata."""
    req = RegistryServer_pb2.ListFeatureViewsRequest(
        project=project,
        allow_cache=allow_cache,
    )
    response = grpc_call(grpc_handler.ListFeatureViews, req)
    feature_views = response.get("featureViews", [])

    jobs: List[Dict[str, Any]] = []
    for fv in feature_views:
        spec = fv.get("spec", fv)
        meta = fv.get("meta", {})
        fv_name = spec.get("name", "unknown")
        intervals = meta.get("materializationIntervals", [])

        for idx, interval in enumerate(intervals):
            start_time = interval.get("startTime", interval.get("start_time"))
            end_time = interval.get("endTime", interval.get("end_time"))

            jobs.append(
                {
                    "id": f"{fv_name}-{idx}",
                    "featureView": fv_name,
                    "status": "SUCCEEDED",
                    "startTime": start_time,
                    "endTime": end_time,
                }
            )

    jobs.sort(key=lambda j: j.get("startTime", ""), reverse=True)
    return jobs


def get_compute_engine_router(grpc_handler, store=None) -> APIRouter:
    router = APIRouter()

    def _fv_to_info(fv: dict, fv_type: str) -> Dict[str, Any]:
        spec = fv.get("spec", fv)
        meta = fv.get("meta", {})
        intervals = meta.get("materializationIntervals", [])
        batch_engine = spec.get("batchEngine")

        last_materialized = None
        if intervals:
            last_interval = intervals[-1]
            last_materialized = last_interval.get(
                "endTime", last_interval.get("end_time")
            )

        return {
            "name": spec.get("name"),
            "type": fv_type,
            "online": spec.get("online", True),
            "lastMaterialized": last_materialized,
            "hasOverride": batch_engine is not None,
            "overrides": batch_engine,
            "materializationIntervals": intervals,
        }

    @router.get("/compute_engines")
    def get_compute_engine(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
    ):
        engine_info = (
            _extract_engine_info(store)
            if store
            else {
                "engineType": "local",
                "engineClass": "LocalComputeEngine",
                "config": {"type": "local"},
            }
        )

        fv_infos: List[Dict[str, Any]] = []

        batch_resp = grpc_call(
            grpc_handler.ListFeatureViews,
            RegistryServer_pb2.ListFeatureViewsRequest(
                project=project, allow_cache=allow_cache
            ),
        )
        for fv in batch_resp.get("featureViews", []):
            fv_infos.append(_fv_to_info(fv, "Batch"))

        stream_resp = grpc_call(
            grpc_handler.ListStreamFeatureViews,
            RegistryServer_pb2.ListStreamFeatureViewsRequest(
                project=project, allow_cache=allow_cache
            ),
        )
        for sfv in stream_resp.get("streamFeatureViews", []):
            fv_infos.append(_fv_to_info(sfv, "Stream"))

        engine_info["featureViewCount"] = len(fv_infos)
        engine_info["project"] = project

        return {
            "engine": engine_info,
            "featureViews": fv_infos,
        }

    @router.get("/compute_engines/all")
    def list_all_compute_engines(
        allow_cache: bool = Query(default=True),
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
    ):
        req = RegistryServer_pb2.ListProjectsRequest(allow_cache=allow_cache)
        projects_resp = grpc_call(grpc_handler.ListProjects, req)
        projects = projects_resp.get("projects", [])

        engines = []
        for proj in projects:
            proj_name = proj.get("spec", {}).get("name") or proj.get("name", "")
            if not proj_name:
                continue

            engine_info = (
                _extract_engine_info(store)
                if store
                else {
                    "engineType": "local",
                    "engineClass": "LocalComputeEngine",
                    "config": {"type": "local"},
                    "mode": "standalone",
                    "configSource": "feature_store.yaml",
                }
            )

            fv_req = RegistryServer_pb2.ListFeatureViewsRequest(
                project=proj_name,
                allow_cache=allow_cache,
            )
            fv_resp = grpc_call(grpc_handler.ListFeatureViews, fv_req)
            fv_count = len(fv_resp.get("featureViews", []))

            engine_info["project"] = proj_name
            engine_info["featureViewCount"] = fv_count
            engines.append(engine_info)

        start = (page - 1) * limit
        end = start + limit
        paginated = engines[start:end]

        return {
            "engines": paginated,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": len(engines),
            },
        }

    @router.get("/materialization_jobs")
    def list_materialization_jobs(
        project: str = Query(...),
        allow_cache: bool = Query(default=True),
        status: Optional[str] = Query(None, description="Filter by status"),
        feature_view: Optional[str] = Query(
            None, alias="feature_view", description="Filter by feature view name"
        ),
        pagination_params: dict = Depends(get_pagination_params),
        sorting_params: dict = Depends(get_sorting_params),
    ):
        jobs = _extract_materialization_jobs(grpc_handler, project, allow_cache)

        if status:
            jobs = [j for j in jobs if j.get("status") == status.upper()]
        if feature_view:
            jobs = [j for j in jobs if j.get("featureView") == feature_view]

        page = pagination_params.get("page", 1)
        limit = pagination_params.get("limit", 50)
        start = (page - 1) * limit
        end = start + limit
        total = len(jobs)
        paginated = jobs[start:end]

        return {
            "jobs": paginated,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
            },
        }

    return router
