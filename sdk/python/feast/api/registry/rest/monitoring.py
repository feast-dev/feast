import logging
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from feast.infra.offline_stores.offline_store import OfflineStore
from feast.permissions.action import AuthzedAction
from feast.permissions.security_manager import assert_permissions

VALID_GRANULARITIES = OfflineStore.MONITORING_VALID_GRANULARITIES


logger = logging.getLogger(__name__)


class ComputeMetricsRequest(BaseModel):
    project: str
    feature_view_name: Optional[str] = None
    feature_names: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    granularity: str = Field("daily")
    set_baseline: bool = False


class AutoComputeRequest(BaseModel):
    project: str
    feature_view_name: Optional[str] = None


class ComputeTransientRequest(BaseModel):
    project: str
    feature_view_name: str
    feature_names: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


def get_monitoring_router(grpc_handler, server=None):
    router = APIRouter()

    def _get_monitoring_service():
        from feast.monitoring.monitoring_service import MonitoringService

        store = server.store if server else grpc_handler.store
        return MonitoringService(store)

    def _get_store():
        return server.store if server else grpc_handler.store

    # ------------------------------------------------------------------ #
    #  DQM Job: submit and track
    # ------------------------------------------------------------------ #

    @router.post("/monitoring/compute", tags=["Monitoring"])
    async def compute_metrics(request: ComputeMetricsRequest):
        """Submit a DQM job to compute and store metrics. Returns job_id."""
        if request.granularity not in VALID_GRANULARITIES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid granularity '{request.granularity}'. "
                f"Must be one of {VALID_GRANULARITIES}",
            )

        store = _get_store()
        if request.feature_view_name:
            fv = store.registry.get_feature_view(
                name=request.feature_view_name, project=request.project
            )
            assert_permissions(fv, actions=[AuthzedAction.UPDATE])

        svc = _get_monitoring_service()

        params = {}
        if request.start_date:
            params["start_date"] = request.start_date
        if request.end_date:
            params["end_date"] = request.end_date
        if request.feature_names:
            params["feature_names"] = request.feature_names
        params["granularity"] = request.granularity
        params["set_baseline"] = request.set_baseline

        job_id = svc.submit_job(
            project=request.project,
            job_type="compute",
            feature_view_name=request.feature_view_name,
            parameters=params,
        )

        # Execute synchronously for now; async worker is a future enhancement
        try:
            result = svc.execute_job(job_id)
            return {"job_id": job_id, **result}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.post("/monitoring/auto_compute", tags=["Monitoring"])
    async def auto_compute(request: AutoComputeRequest):
        """Auto-detect date ranges and compute all granularities."""
        store = _get_store()
        if request.feature_view_name:
            fv = store.registry.get_feature_view(
                name=request.feature_view_name, project=request.project
            )
            assert_permissions(fv, actions=[AuthzedAction.UPDATE])

        svc = _get_monitoring_service()

        job_id = svc.submit_job(
            project=request.project,
            job_type="auto_compute",
            feature_view_name=request.feature_view_name,
        )

        try:
            result = svc.execute_job(job_id)
            return {"job_id": job_id, **result}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/monitoring/jobs/{job_id}", tags=["Monitoring"])
    async def get_job_status(job_id: str):
        svc = _get_monitoring_service()
        job = svc.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return job

    # ------------------------------------------------------------------ #
    #  Transient compute (not stored)
    # ------------------------------------------------------------------ #

    @router.post("/monitoring/compute/transient", tags=["Monitoring"])
    async def compute_transient(request: ComputeTransientRequest):
        """Compute metrics on-the-fly for an arbitrary date range. Results are
        returned directly and NOT persisted to the monitoring tables."""
        store = _get_store()
        fv = store.registry.get_feature_view(
            name=request.feature_view_name, project=request.project
        )
        assert_permissions(fv, actions=[AuthzedAction.DESCRIBE])

        svc = _get_monitoring_service()

        start_d = date.fromisoformat(request.start_date) if request.start_date else None
        end_d = date.fromisoformat(request.end_date) if request.end_date else None

        result = svc.compute_transient(
            project=request.project,
            feature_view_name=request.feature_view_name,
            feature_names=request.feature_names,
            start_date=start_d,
            end_date=end_d,
        )
        return result

    # ------------------------------------------------------------------ #
    #  Read endpoints
    # ------------------------------------------------------------------ #

    @router.get("/monitoring/metrics/features", tags=["Monitoring"])
    async def get_feature_metrics(
        project: str = Query(...),
        feature_view_name: Optional[str] = Query(None),
        feature_name: Optional[str] = Query(None),
        feature_service_name: Optional[str] = Query(None),
        granularity: Optional[str] = Query(None),
        data_source_type: Optional[str] = Query(None),
        start_date: Optional[str] = Query(None),
        end_date: Optional[str] = Query(None),
    ):
        store = _get_store()
        if feature_view_name:
            fv = store.registry.get_feature_view(
                name=feature_view_name, project=project
            )
            assert_permissions(fv, actions=[AuthzedAction.DESCRIBE])

        svc = _get_monitoring_service()
        return svc.get_feature_metrics(
            project=project,
            feature_service_name=feature_service_name,
            feature_view_name=feature_view_name,
            feature_name=feature_name,
            granularity=granularity,
            data_source_type=data_source_type,
            start_date=date.fromisoformat(start_date) if start_date else None,
            end_date=date.fromisoformat(end_date) if end_date else None,
        )

    @router.get("/monitoring/metrics/feature_views", tags=["Monitoring"])
    async def get_feature_view_metrics(
        project: str = Query(...),
        feature_view_name: Optional[str] = Query(None),
        feature_service_name: Optional[str] = Query(None),
        granularity: Optional[str] = Query(None),
        data_source_type: Optional[str] = Query(None),
        start_date: Optional[str] = Query(None),
        end_date: Optional[str] = Query(None),
    ):
        store = _get_store()
        if feature_view_name:
            fv = store.registry.get_feature_view(
                name=feature_view_name, project=project
            )
            assert_permissions(fv, actions=[AuthzedAction.DESCRIBE])

        svc = _get_monitoring_service()
        return svc.get_feature_view_metrics(
            project=project,
            feature_service_name=feature_service_name,
            feature_view_name=feature_view_name,
            granularity=granularity,
            data_source_type=data_source_type,
            start_date=date.fromisoformat(start_date) if start_date else None,
            end_date=date.fromisoformat(end_date) if end_date else None,
        )

    @router.get("/monitoring/metrics/feature_services", tags=["Monitoring"])
    async def get_feature_service_metrics(
        project: str = Query(...),
        feature_service_name: Optional[str] = Query(None),
        granularity: Optional[str] = Query(None),
        data_source_type: Optional[str] = Query(None),
        start_date: Optional[str] = Query(None),
        end_date: Optional[str] = Query(None),
    ):
        store = _get_store()
        if feature_service_name:
            fs = store.registry.get_feature_service(
                name=feature_service_name, project=project
            )
            assert_permissions(fs, actions=[AuthzedAction.DESCRIBE])

        svc = _get_monitoring_service()
        return svc.get_feature_service_metrics(
            project=project,
            feature_service_name=feature_service_name,
            granularity=granularity,
            data_source_type=data_source_type,
            start_date=date.fromisoformat(start_date) if start_date else None,
            end_date=date.fromisoformat(end_date) if end_date else None,
        )

    @router.get("/monitoring/metrics/baseline", tags=["Monitoring"])
    async def get_baseline(
        project: str = Query(...),
        feature_view_name: Optional[str] = Query(None),
        feature_name: Optional[str] = Query(None),
        data_source_type: Optional[str] = Query(None),
    ):
        store = _get_store()
        if feature_view_name:
            fv = store.registry.get_feature_view(
                name=feature_view_name, project=project
            )
            assert_permissions(fv, actions=[AuthzedAction.DESCRIBE])

        svc = _get_monitoring_service()
        return svc.get_baseline(
            project=project,
            feature_view_name=feature_view_name,
            feature_name=feature_name,
            data_source_type=data_source_type,
        )

    @router.get("/monitoring/metrics/timeseries", tags=["Monitoring"])
    async def get_timeseries(
        project: str = Query(...),
        feature_view_name: Optional[str] = Query(None),
        feature_name: Optional[str] = Query(None),
        feature_service_name: Optional[str] = Query(None),
        granularity: Optional[str] = Query(None),
        data_source_type: Optional[str] = Query(None),
        start_date: Optional[str] = Query(None),
        end_date: Optional[str] = Query(None),
    ):
        store = _get_store()
        if feature_view_name:
            fv = store.registry.get_feature_view(
                name=feature_view_name, project=project
            )
            assert_permissions(fv, actions=[AuthzedAction.DESCRIBE])

        svc = _get_monitoring_service()
        return svc.get_timeseries(
            project=project,
            feature_view_name=feature_view_name,
            feature_name=feature_name,
            feature_service_name=feature_service_name,
            granularity=granularity,
            data_source_type=data_source_type,
            start_date=date.fromisoformat(start_date) if start_date else None,
            end_date=date.fromisoformat(end_date) if end_date else None,
        )

    return router
