from unittest.mock import patch

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient

from feast.api.registry.rest.system import get_system_router


@pytest.mark.parametrize("version", ["0.66.0", "unknown"])
def test_version_endpoint_returns_runtime_package_version(version: str) -> None:
    app = FastAPI()
    app.include_router(get_system_router())

    with patch("feast.api.registry.rest.system.get_version", return_value=version):
        response = TestClient(app).get("/version")

    assert response.status_code == 200
    assert response.json() == {"version": version}


def test_version_endpoint_inherits_application_authentication() -> None:
    def require_authentication() -> None:
        raise HTTPException(status_code=401, detail="Authentication required")

    app = FastAPI(dependencies=[Depends(require_authentication)])
    app.include_router(get_system_router())

    response = TestClient(app).get("/version")

    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication required"}
