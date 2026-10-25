"""Authorization helpers shared by the Data Quality Monitoring REST routes.

The monitoring routes invoke :class:`~feast.monitoring.monitoring_service.MonitoringService`
in-process. Unlike the other registry REST routes they cannot simply defer to
``grpc_call``:

* ``grpc_call`` authorizes registry reads with ``AuthzedAction.DESCRIBE`` only,
  while the monitoring *compute* routes require ``UPDATE`` — and no gRPC method
  authorizes ``UPDATE`` without also mutating the registry;
* the monitoring data plane (metric computation, offline-store queries, DQM
  jobs) has no gRPC equivalent at all.

So authorization here is done with the registry/offline layer's *own*
enforcement primitives — :func:`assert_permissions` and
:func:`permitted_resources`, the same functions the gRPC handlers call — inside
a project-scoped context that reproduces what ``grpc_call`` does for the
project. The registry read methods (``list_feature_views`` etc.) perform no
authorization on their own, so every resource a route resolves must be run
through these helpers.
"""

from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, cast

from fastapi import HTTPException
from fastapi.requests import Request

from feast.feast_object import FeastObject
from feast.permissions.action import AuthzedAction
from feast.permissions.security_manager import (
    assert_permissions,
    get_security_manager,
    permitted_resources,
)


@contextmanager
def scoped_project(project: str) -> Iterator[None]:
    """Scope the global ``SecurityManager`` to ``project`` for the block.

    Mirrors ``grpc_call``: binds the request's project to the current context so
    in-process permission checks load the target project's policies, and resets
    the context afterwards. This is what stops a project supplied only in a POST
    body from being evaluated against the server's default-project policies.
    """
    sm = get_security_manager()
    token = None
    if sm is not None:
        token = sm.set_current_project(project or None)
    try:
        yield
    finally:
        if sm is not None and token is not None:
            sm.reset_current_project(token)


def resolve_project(body_project: str, request: Optional[Request]) -> str:
    """Return the authoritative project, rejecting body/query conflicts.

    Monitoring POST handlers take their project from the JSON body. If a
    ``project`` query parameter is also present and disagrees, the request is
    ambiguous (the object would be resolved from one project while permissions
    could be scoped from another) and is rejected.
    """
    if not body_project:
        raise HTTPException(status_code=422, detail="'project' is required")
    if request is not None:
        query_project = request.query_params.get("project")
        if query_project is not None and query_project != body_project:
            raise HTTPException(
                status_code=400,
                detail="Conflicting 'project' value in query string and request body",
            )
    return body_project


# --------------------------------------------------------------------------- #
#  Single-resource authorization (DESCRIBE for reads, UPDATE for compute)
# --------------------------------------------------------------------------- #


def assert_feature_view(store, project: str, name: str, action: AuthzedAction):
    """Resolve and authorize a single feature view, returning it.

    Raises ``FeastObjectNotFoundException`` (404) if it does not exist or
    ``FeastPermissionError`` (403) if the caller lacks ``action``.
    """
    fv = store.registry.get_feature_view(name=name, project=project)
    assert_permissions(fv, actions=[action])
    return fv


def assert_feature_service(store, project: str, name: str, action: AuthzedAction):
    """Resolve and authorize a single feature service, returning it."""
    fs = store.registry.get_feature_service(name=name, project=project)
    assert_permissions(fs, actions=[action])
    return fs


def assert_project(store, project: str, action: AuthzedAction):
    """Authorize a project-level action (used for DQM job metadata)."""
    proj = store.registry.get_project(name=project, allow_cache=True)
    assert_permissions(proj, actions=[action])
    return proj


# --------------------------------------------------------------------------- #
#  Fan-out authorization (selector-free operations)
# --------------------------------------------------------------------------- #


def permitted_feature_view_names(
    store, project: str, action: AuthzedAction
) -> List[str]:
    """Names of the project's feature views the caller may act on.

    Uses ``permitted_resources`` (filter semantics) exactly like the gRPC
    ``ListFeatureViews`` handler, so the result is the caller's authorized
    subset rather than the whole project.
    """
    fvs = store.registry.list_feature_views(project=project)
    allowed = permitted_resources(
        resources=cast(List[FeastObject], list(fvs)), actions=action
    )
    return [fv.name for fv in allowed]


def permitted_feature_service_names(
    store, project: str, action: AuthzedAction
) -> List[str]:
    """Names of the project's feature services the caller may act on."""
    services = store.registry.list_feature_services(project=project)
    allowed = permitted_resources(
        resources=cast(List[FeastObject], list(services)), actions=action
    )
    return [fs.name for fs in allowed]


def filter_rows(
    rows: List[Dict[str, Any]],
    allowed_feature_views: Optional[List[str]] = None,
    allowed_feature_services: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Keep only monitoring rows that reference an authorized resource.

    A row is dropped when it carries a ``feature_view_name`` /
    ``feature_service_name`` that is not in the corresponding allow-list. Rows
    without the relevant key are left untouched (the caller only passes the
    allow-list relevant to the endpoint).
    """
    fv_set = set(allowed_feature_views) if allowed_feature_views is not None else None
    fs_set = (
        set(allowed_feature_services) if allowed_feature_services is not None else None
    )

    result = []
    for row in rows:
        if fv_set is not None:
            fv_name = row.get("feature_view_name")
            if fv_name is not None and fv_name not in fv_set:
                continue
        if fs_set is not None:
            fs_name = row.get("feature_service_name")
            if fs_name is not None and fs_name not in fs_set:
                continue
        result.append(row)
    return result
