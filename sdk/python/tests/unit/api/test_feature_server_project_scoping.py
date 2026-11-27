"""Project-scoping authorization tests for the REST auth dependency.

``scope_to_query_project=False`` (feature server / lineage server) authorizes
against the served project and ignores the ``?project=`` query parameter;
``scope_to_query_project=True`` (registry REST / UI server) scopes permissions
to the requested project.
"""

from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import feast.permissions.auth.auth_manager as auth_manager_module
import feast.permissions.security_manager as security_manager_module
from feast.data_source import DataSource
from feast.errors import FeastPermissionError
from feast.feature_view import FeatureView
from feast.permissions.action import AuthzedAction
from feast.permissions.auth.auth_manager import AuthManager
from feast.permissions.auth.token_extractor import TokenExtractor
from feast.permissions.auth.token_parser import TokenParser
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import SecurityManager, get_security_manager
from feast.permissions.server.rest import build_auth_dependency
from feast.permissions.user import User

SERVED_PROJECT = "prod"
OTHER_PROJECT = "sandbox"
MISSING_PROJECT = "does-not-exist"
FV_NAME = "driver_stats"


def _feature_view(name: str) -> FeatureView:
    source = MagicMock(spec=DataSource)
    source.created_timestamp_column = None
    source.timestamp_field = None
    source.date_partition_column = None
    return FeatureView(name=name, source=source, tags={})


class _HeaderTokenExtractor(TokenExtractor):
    def extract_access_token(self, **kwargs) -> str:
        header = kwargs["request"].headers.get("Authorization")
        return self._extract_bearer_token(header) if header else ""


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


def _permissions_for(project: str) -> List[Permission]:
    """prod: only role ``prod-admin`` may touch feature views.
    sandbox: only role ``sandbox-dev`` may touch feature views.
    Any other project (e.g. a non-existent one): no permissions at all."""
    if project == SERVED_PROJECT:
        role = "prod-admin"
    elif project == OTHER_PROJECT:
        role = "sandbox-dev"
    else:
        return []
    return [
        Permission(
            name=f"{project}-fv",
            types=FeatureView,
            policy=RoleBasedPolicy(roles=[role]),
            actions=[AuthzedAction.WRITE_ONLINE],
        )
    ]


def _make_client(monkeypatch, scope_to_query_project: bool) -> TestClient:
    served_fv = _feature_view(FV_NAME)

    registry = MagicMock()
    registry.list_permissions.side_effect = lambda project, **kw: _permissions_for(
        project
    )

    # monkeypatch restores the previous global managers (including None) on
    # teardown, so these tests do not leak state into unrelated ones.
    monkeypatch.setattr(
        security_manager_module,
        "_sm",
        SecurityManager(project=SERVED_PROJECT, registry=registry),
    )
    monkeypatch.setattr(
        auth_manager_module,
        "_auth_manager",
        _MapAuthManager(
            {
                "alice": ["prod-admin"],
                "mallory": ["sandbox-dev"],
                "eve": ["no-role"],
            }
        ),
    )

    app = FastAPI(
        dependencies=[
            Depends(
                build_auth_dependency(scope_to_query_project=scope_to_query_project)
            )
        ]
    )

    @app.exception_handler(FeastPermissionError)
    async def _perm(request, exc):
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.post("/write-to-online-store")
    def write_to_online_store():
        get_security_manager().assert_permissions(
            resources=[served_fv], actions=[AuthzedAction.WRITE_ONLINE]
        )
        return {"ok": True}

    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture
def feature_server_client(monkeypatch) -> TestClient:
    return _make_client(monkeypatch, scope_to_query_project=False)


@pytest.fixture
def registry_server_client(monkeypatch) -> TestClient:
    return _make_client(monkeypatch, scope_to_query_project=True)


def _auth(user: str) -> dict:
    return {"Authorization": f"Bearer {user}"}


def test_authorized_user_allowed(feature_server_client):
    resp = feature_server_client.post("/write-to-online-store", headers=_auth("alice"))
    assert resp.status_code == 200


def test_other_project_user_denied_without_query_param(feature_server_client):
    resp = feature_server_client.post(
        "/write-to-online-store", headers=_auth("mallory")
    )
    assert resp.status_code == 403


def test_other_project_user_cannot_escalate_via_query_param(feature_server_client):
    resp = feature_server_client.post(
        "/write-to-online-store",
        params={"project": OTHER_PROJECT},
        headers=_auth("mallory"),
    )
    assert resp.status_code == 403


def test_no_role_user_cannot_escalate_via_missing_project(feature_server_client):
    resp = feature_server_client.post(
        "/write-to-online-store",
        params={"project": MISSING_PROJECT},
        headers=_auth("eve"),
    )
    assert resp.status_code == 403


def test_query_param_scoping_sets_requested_project(registry_server_client):
    resp = registry_server_client.post(
        "/write-to-online-store",
        params={"project": OTHER_PROJECT},
        headers=_auth("mallory"),
    )
    assert resp.status_code == 200
