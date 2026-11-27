from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from feast.api.registry.rest.compute_engines import get_compute_engine_router


def test_compute_engine_endpoint_returns_feature_view_state() -> None:
    grpc_handler = MagicMock()
    responses = {
        grpc_handler.ListFeatureViews: {
            "featureViews": [
                {"spec": {"name": "driver_stats"}, "meta": {"state": "MATERIALIZING"}},
                {"spec": {"name": "legacy_view"}, "meta": {}},
            ]
        },
        grpc_handler.ListStreamFeatureViews: {"streamFeatureViews": []},
    }
    app = FastAPI()
    app.include_router(get_compute_engine_router(grpc_handler))

    with patch(
        "feast.api.registry.rest.compute_engines.grpc_call",
        side_effect=lambda method, _req: responses[method],
    ):
        response = TestClient(app).get("/compute_engines?project=test")

    assert response.status_code == 200
    states = {fv["name"]: fv["state"] for fv in response.json()["featureViews"]}
    assert states == {"driver_stats": "MATERIALIZING", "legacy_view": None}
