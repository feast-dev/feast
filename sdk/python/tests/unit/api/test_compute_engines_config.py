"""Tests for the REST registry compute-engine endpoint.

* ``_extract_engine_info`` returns only a small, high-level allowlist of safe
  scalar config fields (e.g. ``region``, ``namespace``, ``warehouse``).
  Credentials, connection identity, location/URL values and nested settings
  such as ``password``, ``account`` or ``spark_conf`` are never returned, and
  an unknown engine type exposes only ``type``.
* ``GET /compute_engines`` is scoped to the requested project: a caller
  permitted on that project is allowed (200), a caller without that permission
  gets 403, and a nonexistent project gets 404.
"""

from types import SimpleNamespace
from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

import feast.permissions.auth.auth_manager as auth_manager_module
import feast.permissions.security_manager as security_manager_module
from feast.api.registry.rest.compute_engines import (
    _extract_engine_info,
    get_compute_engine_router,
)
from feast.errors import FeastObjectNotFoundException, FeastPermissionError
from feast.permissions.action import AuthzedAction
from feast.permissions.auth.auth_manager import AuthManager
from feast.permissions.auth.token_extractor import TokenExtractor
from feast.permissions.auth.token_parser import TokenParser
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import SecurityManager
from feast.permissions.server.rest import build_auth_dependency
from feast.permissions.user import User
from feast.project import Project
from feast.protos.feast.registry import RegistryServer_pb2

FIELD_VALUE = "sample-value"
DEFAULT_PROJECT = "prod"
OTHER_PROJECT = "sandbox"
BATCH_ENGINE = {"type": "snowflake.engine", "database": "DB", "password": FIELD_VALUE}


def _store_with_batch_engine(batch_engine):
    return SimpleNamespace(config=SimpleNamespace(batch_engine=batch_engine))


def test_extract_engine_info_dict_config_excludes_connection_fields():
    store = _store_with_batch_engine(
        {
            "type": "snowflake.engine",
            "database": "DB",
            "password": FIELD_VALUE,
            "private_key_passphrase": FIELD_VALUE,
        }
    )
    info = _extract_engine_info(store)
    assert FIELD_VALUE not in repr(info)
    assert "password" not in info["config"]
    assert "private_key_passphrase" not in info["config"]
    assert info["config"]["database"] == "DB"


def test_extract_engine_info_model_dump_config_excludes_connection_fields():
    class _FakeEngine:
        type = "snowflake.engine"

        def model_dump(self):
            return {
                "type": "snowflake.engine",
                "warehouse": "COMPUTE_WH",
                "database": "FEAST",
                "account": "acct",
                "user": None,
                "password": FIELD_VALUE,
                "private_key_content": FIELD_VALUE,
                "private_key": FIELD_VALUE,
                "auth_token": FIELD_VALUE,
            }

    info = _extract_engine_info(_store_with_batch_engine(_FakeEngine()))
    assert FIELD_VALUE not in repr(info)
    for key in ("password", "private_key", "private_key_content", "auth_token"):
        assert key not in info["config"]
    assert info["config"]["warehouse"] == "COMPUTE_WH"
    assert info["config"]["database"] == "FEAST"
    assert "account" not in info["config"]
    assert "user" not in info["config"]


def test_extract_engine_info_returns_only_allowlisted_scalar_fields():
    store = _store_with_batch_engine(
        {
            "type": "spark.engine",
            "region": "us-east-1",
            "partitions": 4,
            "staging_location": f"s3a://key:{FIELD_VALUE}@bucket/path",
            "spark_conf": {
                "spark.app.name": "feast",
                "spark.hadoop.fs.s3a.secret.key": FIELD_VALUE,
            },
        }
    )
    info = _extract_engine_info(store)
    assert FIELD_VALUE not in repr(info)
    assert info["config"] == {
        "type": "spark.engine",
        "region": "us-east-1",
        "partitions": 4,
    }


def test_extract_engine_info_unknown_engine_returns_only_type():
    store = _store_with_batch_engine(
        {"type": "custom.engine", "endpoint": "example", "password": FIELD_VALUE}
    )
    info = _extract_engine_info(store)
    assert info["config"] == {"type": "custom.engine"}
    assert FIELD_VALUE not in repr(info)


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


class _FakeRegistryHandler:
    """The endpoint lists feature views after the project check; return empty
    responses so a permitted request completes."""

    def ListFeatureViews(self, request, context=None):
        return RegistryServer_pb2.ListFeatureViewsResponse()

    def ListStreamFeatureViews(self, request, context=None):
        return RegistryServer_pb2.ListStreamFeatureViewsResponse()


def _permissions_for(project: str) -> List[Permission]:
    role = {DEFAULT_PROJECT: "prod-admin", OTHER_PROJECT: "sandbox-reader"}.get(project)
    if role is None:
        return []
    return [
        Permission(
            name=f"{project}-proj",
            types=Project,
            policy=RoleBasedPolicy(roles=[role]),
            actions=[AuthzedAction.DESCRIBE],
        )
    ]


def _get_project(name, **kwargs):
    if name in (DEFAULT_PROJECT, OTHER_PROJECT):
        return Project(name=name)
    raise FeastObjectNotFoundException(f"Project {name} not found")


@pytest.fixture
def client(monkeypatch) -> TestClient:
    registry = MagicMock()
    registry.list_permissions.side_effect = lambda project, **kw: _permissions_for(
        project
    )
    registry.get_project.side_effect = _get_project

    store = SimpleNamespace(
        config=SimpleNamespace(batch_engine=BATCH_ENGINE), registry=registry
    )

    # monkeypatch restores the previous global managers (including None) on
    # teardown, so these tests do not leak state into unrelated ones.
    monkeypatch.setattr(
        security_manager_module,
        "_sm",
        SecurityManager(project=DEFAULT_PROJECT, registry=registry),
    )
    monkeypatch.setattr(
        auth_manager_module,
        "_auth_manager",
        _MapAuthManager({"alice": ["sandbox-reader"], "bob": ["no-role"]}),
    )

    app = FastAPI(
        dependencies=[Depends(build_auth_dependency(scope_to_query_project=True))]
    )

    @app.exception_handler(FeastPermissionError)
    async def _perm(request, exc):
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(FeastObjectNotFoundException)
    async def _missing(request, exc):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    app.include_router(get_compute_engine_router(_FakeRegistryHandler(), store=store))
    return TestClient(app, raise_server_exceptions=True)


def _auth(user: str) -> dict:
    return {"Authorization": f"Bearer {user}"}


def test_user_allowed_for_their_own_project(client):
    # alice is permitted only on OTHER_PROJECT, so the request is scoped to that
    # project's policies (not the server's default project) and succeeds.
    resp = client.get(
        "/compute_engines", params={"project": OTHER_PROJECT}, headers=_auth("alice")
    )
    assert resp.status_code == 200
    assert FIELD_VALUE not in resp.text


def test_user_denied_for_project_without_permission(client):
    resp = client.get(
        "/compute_engines", params={"project": DEFAULT_PROJECT}, headers=_auth("alice")
    )
    assert resp.status_code == 403
    assert FIELD_VALUE not in resp.text


def test_nonexistent_project_returns_not_found(client):
    resp = client.get(
        "/compute_engines",
        params={"project": "does-not-exist"},
        headers=_auth("bob"),
    )
    assert resp.status_code == 404
    assert FIELD_VALUE not in resp.text


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
