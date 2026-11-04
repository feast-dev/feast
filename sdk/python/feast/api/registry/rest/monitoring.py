import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from feast.api.registry.rest.monitoring_authz import (
    assert_feature_service,
    assert_feature_view,
    assert_project,
    filter_rows,
    permitted_feature_service_names,
    permitted_feature_view_names,
    resolve_project,
    scoped_project,
)
from feast.errors import FeastPermissionError
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.permissions.action import AuthzedAction

logger = logging.getLogger(__name__)

VALID_GRANULARITIES = OfflineStore.MONITORING_VALID_GRANULARITIES


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


class ComputeLogMetricsRequest(BaseModel):
    project: str
    feature_service_name: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    granularity: str = Field("daily")
    set_baseline: bool = False


class AutoComputeLogRequest(BaseModel):
    project: str
    feature_service_name: Optional[str] = None


class ComputeTransientRequest(BaseModel):
    project: str
    feature_view_name: str
    feature_names: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


def get_monitoring_router(grpc_handler, store=None):
    router = APIRouter()

    _monitoring_service = None

    def _get_monitoring_service():
        nonlocal _monitoring_service
        if _monitoring_service is None:
            if store is None:
                raise HTTPException(
                    status_code=503,
                    detail="Monitoring service is not available: no FeatureStore configured",
                )
            dqm_config = getattr(store.config, "data_quality_monitoring_config", None)
            if dqm_config is None:
                raise HTTPException(
                    status_code=503,
                    detail="Monitoring is not enabled: add data_quality_monitoring section to feature_store.yaml",
                )
            from feast.monitoring.monitoring_service import MonitoringService

            _monitoring_service = MonitoringService(store)
        return _monitoring_service

    def _get_store():
        if store is None:
            raise HTTPException(
                status_code=503,
                detail="Monitoring service is not available: no FeatureStore configured",
            )
        return store

    # ------------------------------------------------------------------ #
    #  Authorization helpers (must run inside ``scoped_project``)
    # ------------------------------------------------------------------ #

    def _authz_compute_feature_views(
        store, project: str, feature_view_name: Optional[str]
    ) -> Optional[List[str]]:
        """Authorize a feature-view computation (UPDATE).

        Returns ``None`` when a single named view is authorized (the service
        resolves it by name). For a selector-free run, returns the caller's
        authorized subset of the project's feature views, or raises 403 when
        that subset is empty.
        """
        if feature_view_name:
            assert_feature_view(store, project, feature_view_name, AuthzedAction.UPDATE)
            return None
        names = permitted_feature_view_names(store, project, AuthzedAction.UPDATE)
        if not names:
            raise FeastPermissionError(
                "Not authorized to compute monitoring metrics for any feature "
                f"view in project '{project}'."
            )
        return names

    def _authz_compute_feature_services(
        store, project: str, feature_service_name: Optional[str]
    ) -> Optional[List[str]]:
        """Authorize a feature-service computation (UPDATE). See above."""
        if feature_service_name:
            assert_feature_service(
                store, project, feature_service_name, AuthzedAction.UPDATE
            )
            return None
        names = permitted_feature_service_names(store, project, AuthzedAction.UPDATE)
        if not names:
            raise FeastPermissionError(
                "Not authorized to compute monitoring metrics for any feature "
                f"service in project '{project}'."
            )
        return names

    def _authz_read_feature_views(
        store,
        project: str,
        feature_view_name: Optional[str],
        feature_service_name: Optional[str],
    ) -> Optional[List[str]]:
        """Authorize a feature-view-keyed read (DESCRIBE).

        Returns ``None`` when an authorized selector already scopes the result,
        otherwise the allow-list of feature-view names to filter rows by.
        Raises 403 when the caller has no permitted feature views at all
        (prevents silent empty responses that could mask a misconfiguration).
        """
        if feature_view_name:
            assert_feature_view(
                store, project, feature_view_name, AuthzedAction.DESCRIBE
            )
            return None
        if feature_service_name:
            assert_feature_service(
                store, project, feature_service_name, AuthzedAction.DESCRIBE
            )
            return None
        names = permitted_feature_view_names(store, project, AuthzedAction.DESCRIBE)
        if not names:
            raise FeastPermissionError(
                "Not authorized to read monitoring metrics for any feature "
                f"view in project '{project}'."
            )
        return names

    def _authz_read_feature_services(
        store, project: str, feature_service_name: Optional[str]
    ) -> Optional[List[str]]:
        """Authorize a feature-service-keyed read (DESCRIBE). See above."""
        if feature_service_name:
            assert_feature_service(
                store, project, feature_service_name, AuthzedAction.DESCRIBE
            )
            return None
        names = permitted_feature_service_names(store, project, AuthzedAction.DESCRIBE)
        if not names:
            raise FeastPermissionError(
                "Not authorized to read monitoring metrics for any feature "
                f"service in project '{project}'."
            )
        return names

    # ------------------------------------------------------------------ #
    #  DQM Job: submit and track
    # ------------------------------------------------------------------ #

    @router.post("/monitoring/compute", tags=["Monitoring"])
    async def compute_metrics(request: ComputeMetricsRequest, http_request: Request):
        """Submit a DQM job to compute and store metrics. Returns job_id.

        When set_baseline is True and no date range is provided, computes
        baseline from all available source data.
        """
        store = _get_store()
        project = resolve_project(request.project, http_request)
        with scoped_project(project):
            allowed_fvs = _authz_compute_feature_views(
                store, project, request.feature_view_name
            )

            svc = _get_monitoring_service()

            if request.set_baseline and not request.start_date and not request.end_date:
                try:
                    result = svc.compute_baseline(
                        project=project,
                        feature_view_name=request.feature_view_name,
                        feature_names=request.feature_names,
                        allowed_feature_view_names=allowed_fvs,
                    )
                    return result
                except Exception as e:
                    raise HTTPException(status_code=500, detail=str(e))

            if request.granularity not in VALID_GRANULARITIES:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid granularity '{request.granularity}'. "
                    f"Must be one of {VALID_GRANULARITIES}",
                )

            params: Dict[str, Any] = {}
            if request.start_date:
                params["start_date"] = request.start_date
            if request.end_date:
                params["end_date"] = request.end_date
            if request.feature_names:
                params["feature_names"] = request.feature_names
            params["granularity"] = request.granularity
            params["set_baseline"] = request.set_baseline
            if allowed_fvs is not None:
                params["allowed_feature_view_names"] = allowed_fvs

            job_id = svc.submit_job(
                project=project,
                job_type="compute",
                feature_view_name=request.feature_view_name,
                parameters=params,
            )

            try:
                result = svc.execute_job(job_id)
                return {"job_id": job_id, **result}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

    @router.post("/monitoring/auto_compute", tags=["Monitoring"])
    async def auto_compute(request: AutoComputeRequest, http_request: Request):
        """Auto-detect date ranges and compute all granularities."""
        store = _get_store()
        project = resolve_project(request.project, http_request)
        with scoped_project(project):
            allowed_fvs = _authz_compute_feature_views(
                store, project, request.feature_view_name
            )

            svc = _get_monitoring_service()

            parameters = (
                {"allowed_feature_view_names": allowed_fvs}
                if allowed_fvs is not None
                else None
            )
            job_id = svc.submit_job(
                project=project,
                job_type="auto_compute",
                feature_view_name=request.feature_view_name,
                parameters=parameters,
            )

            try:
                result = svc.execute_job(job_id)
                return {"job_id": job_id, **result}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

    # ------------------------------------------------------------------ #
    #  Log source: compute from feature serving logs
    # ------------------------------------------------------------------ #

    @router.post("/monitoring/compute/log", tags=["Monitoring"])
    async def compute_log_metrics(
        request: ComputeLogMetricsRequest, http_request: Request
    ):
        """Compute metrics from feature serving logs for a feature service."""
        if request.granularity not in VALID_GRANULARITIES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid granularity '{request.granularity}'. "
                f"Must be one of {VALID_GRANULARITIES}",
            )

        store = _get_store()
        project = resolve_project(request.project, http_request)
        with scoped_project(project):
            # feature_service_name is required on this route; authorize it directly.
            assert_feature_service(
                store, project, request.feature_service_name, AuthzedAction.UPDATE
            )

            svc = _get_monitoring_service()

            start_d = (
                date.fromisoformat(request.start_date) if request.start_date else None
            )
            end_d = date.fromisoformat(request.end_date) if request.end_date else None

            try:
                result = svc.compute_log_metrics(
                    project=project,
                    feature_service_name=request.feature_service_name,
                    start_date=start_d,
                    end_date=end_d,
                    granularity=request.granularity,
                    set_baseline=request.set_baseline,
                )
                return result
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

    @router.post("/monitoring/auto_compute/log", tags=["Monitoring"])
    async def auto_compute_log(request: AutoComputeLogRequest, http_request: Request):
        """Auto-detect date ranges from log data and compute all granularities."""
        store = _get_store()
        project = resolve_project(request.project, http_request)
        with scoped_project(project):
            allowed_fss = _authz_compute_feature_services(
                store, project, request.feature_service_name
            )

            svc = _get_monitoring_service()
            try:
                result = svc.auto_compute_log_metrics(
                    project=project,
                    feature_service_name=request.feature_service_name,
                    allowed_feature_service_names=allowed_fss,
                )
                return result
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

    @router.get("/monitoring/jobs/{job_id}", tags=["Monitoring"])
    async def get_job_status(job_id: str, project: str = Query(...)):
        store = _get_store()
        with scoped_project(project):
            # Gate existence probing on project-level read access before any
            # job metadata (including whether the id exists) is revealed.
            assert_project(store, project, AuthzedAction.DESCRIBE)

            svc = _get_monitoring_service()
            job = svc.get_job(job_id, project=project)
            if job is None:
                raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

            # If the job targets a specific feature view, require describe
            # permission on that feature view too.
            fv_name = job.get("feature_view_name")
            if fv_name:
                assert_feature_view(store, project, fv_name, AuthzedAction.DESCRIBE)
            return job

    # ------------------------------------------------------------------ #
    #  Transient compute (not stored)
    # ------------------------------------------------------------------ #

    @router.post("/monitoring/compute/transient", tags=["Monitoring"])
    async def compute_transient(
        request: ComputeTransientRequest, http_request: Request
    ):
        """Compute metrics on-the-fly for an arbitrary date range. Results are
        returned directly and NOT persisted to the monitoring tables."""
        store = _get_store()
        project = resolve_project(request.project, http_request)
        with scoped_project(project):
            # feature_view_name is required on this route; authorize it directly.
            assert_feature_view(
                store, project, request.feature_view_name, AuthzedAction.DESCRIBE
            )

            svc = _get_monitoring_service()

            start_d = (
                date.fromisoformat(request.start_date) if request.start_date else None
            )
            end_d = date.fromisoformat(request.end_date) if request.end_date else None

            result = svc.compute_transient(
                project=project,
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
        with scoped_project(project):
            allowed_fvs = _authz_read_feature_views(
                store, project, feature_view_name, feature_service_name
            )

            svc = _get_monitoring_service()
            rows = svc.get_feature_metrics(
                project=project,
                feature_service_name=feature_service_name,
                feature_view_name=feature_view_name,
                feature_name=feature_name,
                granularity=granularity,
                data_source_type=data_source_type,
                start_date=date.fromisoformat(start_date) if start_date else None,
                end_date=date.fromisoformat(end_date) if end_date else None,
            )
            return filter_rows(rows, allowed_feature_views=allowed_fvs)

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
        with scoped_project(project):
            allowed_fvs = _authz_read_feature_views(
                store, project, feature_view_name, feature_service_name
            )

            svc = _get_monitoring_service()
            rows = svc.get_feature_view_metrics(
                project=project,
                feature_service_name=feature_service_name,
                feature_view_name=feature_view_name,
                granularity=granularity,
                data_source_type=data_source_type,
                start_date=date.fromisoformat(start_date) if start_date else None,
                end_date=date.fromisoformat(end_date) if end_date else None,
            )
            return filter_rows(rows, allowed_feature_views=allowed_fvs)

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
        with scoped_project(project):
            allowed_fss = _authz_read_feature_services(
                store, project, feature_service_name
            )

            svc = _get_monitoring_service()
            rows = svc.get_feature_service_metrics(
                project=project,
                feature_service_name=feature_service_name,
                granularity=granularity,
                data_source_type=data_source_type,
                start_date=date.fromisoformat(start_date) if start_date else None,
                end_date=date.fromisoformat(end_date) if end_date else None,
            )
            return filter_rows(rows, allowed_feature_services=allowed_fss)

    @router.get("/monitoring/metrics/baseline", tags=["Monitoring"])
    async def get_baseline(
        project: str = Query(...),
        feature_view_name: Optional[str] = Query(None),
        feature_name: Optional[str] = Query(None),
        data_source_type: Optional[str] = Query(None),
    ):
        store = _get_store()
        with scoped_project(project):
            allowed_fvs = _authz_read_feature_views(
                store, project, feature_view_name, None
            )

            svc = _get_monitoring_service()
            rows = svc.get_baseline(
                project=project,
                feature_view_name=feature_view_name,
                feature_name=feature_name,
                data_source_type=data_source_type,
            )
            return filter_rows(rows, allowed_feature_views=allowed_fvs)

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
        with scoped_project(project):
            allowed_fvs = _authz_read_feature_views(
                store, project, feature_view_name, feature_service_name
            )

            svc = _get_monitoring_service()
            rows = svc.get_timeseries(
                project=project,
                feature_view_name=feature_view_name,
                feature_name=feature_name,
                feature_service_name=feature_service_name,
                granularity=granularity,
                data_source_type=data_source_type,
                start_date=date.fromisoformat(start_date) if start_date else None,
                end_date=date.fromisoformat(end_date) if end_date else None,
            )
            return filter_rows(rows, allowed_feature_views=allowed_fvs)

    return router
