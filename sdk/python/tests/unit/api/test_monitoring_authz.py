"""Deny-first authorization tests for the Data Quality Monitoring REST routes.

These tests exercise the real FastAPI monitoring router, the real
``inject_user_details`` auth dependency, a real ``SecurityManager`` with
role-based policies, and the real permission enforcer (``assert_permissions`` /
``permitted_resources``). The ``MonitoringService`` is replaced by an inert
canary so that a ``200`` response proves only that the route completed its
authorization path and reached the service — never that it touched a real
offline store.

Coverage (the monitoring-authz report matrix):

* reads/computes with an omitted selector must be scoped to the caller's
  authorized resources — reads are *filtered* to permitted feature views /
  services, computes run only over the permitted subset (and are denied when
  that subset is empty);
* a supplied (protected) feature service must be authorized;
* the job-status endpoint requires a project and authorizes it;
* a victim-project resource named only in the POST body is authorized against
  the *victim* project's policies, not the server's default project
  (``scoped_project`` replicates ``grpc_call``'s project scoping);
* conflicting body/query projects are rejected; anonymous callers get 401.
"""

from typing import Dict, List, Optional
from unittest.mock import MagicMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from feast.data_source import DataSource
from feast.errors import FeastObjectNotFoundException, FeastPermissionError
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.permissions.auth.auth_manager import AuthManager, set_auth_manager
from feast.permissions.auth.token_extractor import TokenExtractor
from feast.permissions.auth.token_parser import TokenParser
from feast.permissions.permission import AuthzedAction, Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import (
    SecurityManager,
    no_security_manager,
    set_security_manager,
)
from feast.permissions.server.rest import inject_user_details
from feast.permissions.user import User
from feast.project import Project

VICTIM_PROJECT = "victim"
DEFAULT_PROJECT = "default"
VICTIM_FV = "victim_fv"
VICTIM_FS = "victim_fs"
OTHER_FV = "other_fv"
OTHER_FS = "other_fs"


def _secured_feature_view(name: str) -> FeatureView:
    source = MagicMock(spec=DataSource)
    source.created_timestamp_column = None
    source.timestamp_field = None
    source.date_partition_column = None
    return FeatureView(name=name, source=source, tags={})


class _HeaderTokenExtractor(TokenExtractor):
    def extract_access_token(self, **kwargs) -> str:
        request = kwargs["request"]
        header = request.headers.get("Authorization")
        if not header:
            return ""
        return self._extract_bearer_token(header)


class _MapTokenParser(TokenParser):
    def __init__(self, users: Dict[str, List[str]]):
        self._users = users

    async def user_details_from_access_token(self, access_token: str, **kwargs) -> User:
        return User(username=access_token, roles=self._users.get(access_token, []))


class _MapAuthManager(AuthManager):
    def __init__(self, users: Dict[str, List[str]]):
        super().__init__(
            token_parser=_MapTokenParser(users),
            token_extractor=_HeaderTokenExtractor(),
        )


class _CanaryMonitoringService:
    """Inert stand-in: reads return rows for two resources so the route's
    filter-to-permitted step is observable; everything else returns a sentinel.
    Touches no offline store."""

    def __init__(self, store=None):
        self.calls: List[str] = []
        self.kwargs: Dict[str, dict] = {}

    def _rows(self, name: str, **kwargs) -> List[dict]:
        self.calls.append(name)
        self.kwargs[name] = kwargs
        return [
            {
                "feature_view_name": VICTIM_FV,
                "feature_service_name": VICTIM_FS,
                "value": 1,
            },
            {
                "feature_view_name": OTHER_FV,
                "feature_service_name": OTHER_FS,
                "value": 2,
            },
        ]

    def get_feature_metrics(self, **kw):
        return self._rows("get_feature_metrics", **kw)

    def get_feature_view_metrics(self, **kw):
        return self._rows("get_feature_view_metrics", **kw)

    def get_feature_service_metrics(self, **kw):
        return self._rows("get_feature_service_metrics", **kw)

    def get_baseline(self, **kw):
        return self._rows("get_baseline", **kw)

    def get_timeseries(self, **kw):
        return self._rows("get_timeseries", **kw)

    def compute_baseline(self, **kw):
        self.calls.append("compute_baseline")
        self.kwargs["compute_baseline"] = kw
        return {"status": "completed"}

    def compute_log_metrics(self, **kw):
        self.calls.append("compute_log_metrics")
        self.kwargs["compute_log_metrics"] = kw
        return {"status": "completed"}

    def auto_compute_log_metrics(self, **kw):
        self.calls.append("auto_compute_log_metrics")
        self.kwargs["auto_compute_log_metrics"] = kw
        return {"status": "completed"}

    def compute_transient(self, **kw):
        self.calls.append("compute_transient")
        self.kwargs["compute_transient"] = kw
        return {"status": "completed"}

    def submit_job(self, **kw):
        self.calls.append("submit_job")
        self.kwargs["submit_job"] = kw
        return "job-123"

    def execute_job(self, job_id):
        self.calls.append("execute_job")
        return {"status": "completed"}

    def get_job(self, job_id, project=None):
        self.calls.append("get_job")
        return {
            "job_id": job_id,
            "project_id": project,
            "feature_view_name": None,
            "status": "completed",
        }


def _build_permissions(project: str) -> List[Permission]:
    """Admin-only policies in the victim project; open policies elsewhere.

    The victim-project policies carry no name_patterns, so an unauthorized
    caller is *denied* (raising) rather than silently filtered — matching how
    Feast's enforcer treats pattern-less resource-type permissions.
    """
    granted_role = "admin" if project == VICTIM_PROJECT else "restricted"
    actions = [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE]
    return [
        Permission(
            name=f"{project}-fv",
            types=FeatureView,
            policy=RoleBasedPolicy(roles=[granted_role]),
            actions=actions,
        ),
        Permission(
            name=f"{project}-fs",
            types=FeatureService,
            policy=RoleBasedPolicy(roles=[granted_role]),
            actions=actions,
        ),
        Permission(
            name=f"{project}-project",
            types=Project,
            policy=RoleBasedPolicy(roles=[granted_role]),
            actions=actions,
        ),
    ]


@pytest.fixture
def client(monkeypatch) -> TestClient:
    victim_fv = _secured_feature_view(VICTIM_FV)
    victim_fs = FeatureService(name=VICTIM_FS, features=[victim_fv])

    def _get_feature_view(name, project, **kwargs):
        if name == VICTIM_FV:
            return victim_fv
        raise FeastObjectNotFoundException(f"Feature view {name} not found")

    def _get_feature_service(name, project, **kwargs):
        if name == VICTIM_FS:
            return victim_fs
        raise FeastObjectNotFoundException(f"Feature service {name} not found")

    registry = MagicMock()
    registry.list_permissions.side_effect = lambda project, **kw: _build_permissions(
        project
    )
    registry.get_feature_view.side_effect = _get_feature_view
    registry.get_feature_service.side_effect = _get_feature_service
    registry.get_project.side_effect = lambda name, **kw: Project(name=name)
    registry.list_feature_views.side_effect = lambda project, **kw: [victim_fv]
    registry.list_feature_services.side_effect = lambda project, **kw: [victim_fs]

    store = MagicMock()
    store.config.project = DEFAULT_PROJECT
    store.config.data_quality_monitoring_config = {"enabled": True}
    store.registry = registry

    # Replace the real service with the canary (imported lazily inside the route).
    monkeypatch.setattr(
        "feast.monitoring.monitoring_service.MonitoringService",
        _CanaryMonitoringService,
    )

    sm = SecurityManager(project=DEFAULT_PROJECT, registry=registry)
    set_security_manager(sm)
    set_auth_manager(
        _MapAuthManager({"restricted": ["restricted"], "admin": ["admin"]})
    )

    from feast.api.registry.rest.monitoring import get_monitoring_router

    app = FastAPI(dependencies=[Depends(inject_user_details)])

    @app.exception_handler(FeastPermissionError)
    async def _perm(request, exc):
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(FeastObjectNotFoundException)
    async def _notfound(request, exc):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    app.include_router(get_monitoring_router(grpc_handler=None, store=store))

    yield TestClient(app, raise_server_exceptions=True)

    no_security_manager()


def _auth(user: Optional[str]) -> dict:
    return {"Authorization": f"Bearer {user}"} if user else {}


# --------------------------------------------------------------------------- #
#  Anonymous control
# --------------------------------------------------------------------------- #


def test_anonymous_is_unauthorized(client):
    resp = client.get(
        "/monitoring/metrics/features", params={"project": VICTIM_PROJECT}
    )
    assert resp.status_code == 401


# --------------------------------------------------------------------------- #
#  Reads
# --------------------------------------------------------------------------- #


def test_read_all_features_without_selector_denied(client):
    # Omitting every selector must scope to permitted resources; the restricted
    # user is permitted none (pattern-less deny), so the enforcer raises -> 403.
    resp = client.get(
        "/monitoring/metrics/features",
        params={"project": VICTIM_PROJECT},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_read_protected_feature_view_denied(client):
    resp = client.get(
        "/monitoring/metrics/features",
        params={"project": VICTIM_PROJECT, "feature_view_name": VICTIM_FV},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_read_through_protected_feature_service_denied(client):
    # The feature-service selector must itself be authorized.
    resp = client.get(
        "/monitoring/metrics/features",
        params={"project": VICTIM_PROJECT, "feature_service_name": VICTIM_FS},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_read_all_baselines_denied(client):
    resp = client.get(
        "/monitoring/metrics/baseline",
        params={"project": VICTIM_PROJECT},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_read_all_timeseries_denied(client):
    resp = client.get(
        "/monitoring/metrics/timeseries",
        params={"project": VICTIM_PROJECT},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_read_all_feature_views_without_selector_denied(client):
    resp = client.get(
        "/monitoring/metrics/feature_views",
        params={"project": VICTIM_PROJECT},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_read_all_feature_services_without_selector_denied(client):
    resp = client.get(
        "/monitoring/metrics/feature_services",
        params={"project": VICTIM_PROJECT},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_admin_read_all_features_filtered_to_permitted(client):
    # Positive control + filter proof: the admin is permitted VICTIM_FV, so the
    # OTHER_FV row the service returns is filtered out.
    resp = client.get(
        "/monitoring/metrics/features",
        params={"project": VICTIM_PROJECT},
        headers=_auth("admin"),
    )
    assert resp.status_code == 200
    rows = resp.json()
    assert [r["feature_view_name"] for r in rows] == [VICTIM_FV]


def test_admin_read_all_feature_services_filtered_to_permitted(client):
    resp = client.get(
        "/monitoring/metrics/feature_services",
        params={"project": VICTIM_PROJECT},
        headers=_auth("admin"),
    )
    assert resp.status_code == 200
    rows = resp.json()
    assert [r["feature_service_name"] for r in rows] == [VICTIM_FS]


# --------------------------------------------------------------------------- #
#  Computes
# --------------------------------------------------------------------------- #


def test_compute_all_feature_views_denied(client):
    resp = client.post(
        "/monitoring/compute",
        json={"project": VICTIM_PROJECT, "granularity": "daily"},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_compute_protected_feature_view_denied(client):
    resp = client.post(
        "/monitoring/compute",
        json={
            "project": VICTIM_PROJECT,
            "feature_view_name": VICTIM_FV,
            "granularity": "daily",
        },
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_auto_compute_log_all_services_denied(client):
    resp = client.post(
        "/monitoring/auto_compute/log",
        json={"project": VICTIM_PROJECT},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_auto_compute_log_protected_service_denied(client):
    resp = client.post(
        "/monitoring/auto_compute/log",
        json={"project": VICTIM_PROJECT, "feature_service_name": VICTIM_FS},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_admin_compute_all_feature_views_allowed(client):
    # Positive control: project-permitted UPDATE lets the authorized user run
    # the selector-free computation.
    resp = client.post(
        "/monitoring/compute",
        json={"project": VICTIM_PROJECT, "granularity": "daily"},
        headers=_auth("admin"),
    )
    assert resp.status_code == 200


# --------------------------------------------------------------------------- #
#  Job metadata
# --------------------------------------------------------------------------- #


def test_job_status_requires_project(client):
    resp = client.get("/monitoring/jobs/some-uuid", headers=_auth("restricted"))
    assert resp.status_code == 422  # missing required project query param


def test_job_status_without_authorization_denied(client):
    resp = client.get(
        "/monitoring/jobs/some-uuid",
        params={"project": VICTIM_PROJECT},
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_job_status_authorized(client):
    resp = client.get(
        "/monitoring/jobs/some-uuid",
        params={"project": VICTIM_PROJECT},
        headers=_auth("admin"),
    )
    assert resp.status_code == 200


# --------------------------------------------------------------------------- #
#  Cross-project scoping (the core of the report)
# --------------------------------------------------------------------------- #


def test_victim_view_named_only_in_post_body_denied(client):
    # No ?project query param: the only project value is in the JSON body.
    # The handler must scope permissions to the *body* project (victim), whose
    # policies deny the restricted user — not the server's default project,
    # whose policies would grant them.
    resp = client.post(
        "/monitoring/compute",
        json={
            "project": VICTIM_PROJECT,
            "feature_view_name": VICTIM_FV,
            "granularity": "daily",
        },
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_victim_view_with_matching_query_project_denied(client):
    resp = client.post(
        "/monitoring/compute",
        params={"project": VICTIM_PROJECT},
        json={
            "project": VICTIM_PROJECT,
            "feature_view_name": VICTIM_FV,
            "granularity": "daily",
        },
        headers=_auth("restricted"),
    )
    assert resp.status_code == 403


def test_conflicting_body_and_query_project_rejected(client):
    resp = client.post(
        "/monitoring/compute",
        params={"project": DEFAULT_PROJECT},
        json={
            "project": VICTIM_PROJECT,
            "feature_view_name": VICTIM_FV,
            "granularity": "daily",
        },
        headers=_auth("restricted"),
    )
    assert resp.status_code == 400


@pytest.mark.parametrize(
    "path,body",
    [
        (
            "/monitoring/compute",
            {"project": VICTIM_PROJECT, "granularity": "daily"},
        ),
        (
            "/monitoring/auto_compute",
            {"project": VICTIM_PROJECT},
        ),
        (
            "/monitoring/auto_compute/log",
            {"project": VICTIM_PROJECT},
        ),
        (
            "/monitoring/compute/log",
            {
                "project": VICTIM_PROJECT,
                "feature_service_name": VICTIM_FS,
                "granularity": "daily",
            },
        ),
        (
            "/monitoring/compute/transient",
            {"project": VICTIM_PROJECT, "feature_view_name": VICTIM_FV},
        ),
    ],
    ids=[
        "compute",
        "auto_compute",
        "auto_compute_log",
        "compute_log",
        "compute_transient",
    ],
)
def test_all_post_routes_reject_project_mismatch(client, path, body):
    """Every POST route must reject conflicting query-string and body projects."""
    resp = client.post(
        path,
        params={"project": DEFAULT_PROJECT},
        json=body,
        headers=_auth("admin"),
    )
    assert resp.status_code == 400, f"{path} did not reject conflicting project values"


def test_restricted_user_allowed_in_default_project(client):
    # Proves the deny above is project-specific scoping, not a blanket deny:
    # the same restricted user IS granted in the default project.
    resp = client.post(
        "/monitoring/compute",
        json={
            "project": DEFAULT_PROJECT,
            "feature_view_name": VICTIM_FV,
            "granularity": "daily",
        },
        headers=_auth("restricted"),
    )
    assert resp.status_code == 200


# --------------------------------------------------------------------------- #
#  Unit tests for the plumbing that constrains selector-free operations
# --------------------------------------------------------------------------- #


def test_resolve_feature_views_honors_allowlist():
    from feast.monitoring.monitoring_service import MonitoringService

    store = MagicMock()
    store.registry.get_feature_view.side_effect = lambda name, project: f"fv:{name}"
    store.registry.list_feature_views.side_effect = lambda project: ["all1", "all2"]

    svc = MonitoringService.__new__(MonitoringService)
    svc._store = store

    # A single named view resolves to just that view.
    assert svc._resolve_feature_views("p", "x") == ["fv:x"]
    # An explicit allow-list resolves to exactly those views (the authorized
    # subset), never the full project listing.
    assert svc._resolve_feature_views("p", None, ["a", "b"]) == ["fv:a", "fv:b"]
    # An empty allow-list computes over nothing (not "everything").
    assert svc._resolve_feature_views("p", None, []) == []
    # No selector and no allow-list keeps the original list-all behavior.
    assert svc._resolve_feature_views("p", None) == ["all1", "all2"]


def test_filter_rows_drops_unauthorized_resources():
    from feast.api.registry.rest.monitoring_authz import filter_rows

    rows = [
        {"feature_view_name": "a"},
        {"feature_view_name": "b"},
        {"feature_service_name": "s"},
    ]
    # Only rows for permitted feature views survive; rows without the key pass.
    assert filter_rows(rows, allowed_feature_views=["a"]) == [
        {"feature_view_name": "a"},
        {"feature_service_name": "s"},
    ]
    # None means "selector already authorized" -> no filtering.
    assert filter_rows(rows) == rows
    # An empty allow-list drops every feature-view-keyed row.
    assert filter_rows(rows, allowed_feature_views=[]) == [
        {"feature_service_name": "s"}
    ]
