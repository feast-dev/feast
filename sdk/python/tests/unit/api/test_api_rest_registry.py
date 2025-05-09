import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.api.registry.rest.rest_registry_server import RestRegistryServer
from feast.feature_store import FeatureStore
from feast.repo_config import RepoConfig
from feast.types import Float64, Int64
from feast.value_type import ValueType


@pytest.fixture
def fastapi_test_app():
    # Create temp registry and data directory
    tmp_dir = tempfile.TemporaryDirectory()
    registry_path = os.path.join(tmp_dir.name, "registry.db")

    # Create dummy parquet file (Feast requires valid sources)
    parquet_file_path = os.path.join(tmp_dir.name, "data.parquet")
    import pandas as pd

    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "age": [25, 30, 22],
            "income": [50000.0, 60000.0, 45000.0],
            "event_timestamp": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03"]
            ),
        }
    )
    df.to_parquet(parquet_file_path)

    # Setup minimal repo config
    config = {
        "registry": registry_path,
        "project": "demo_project",
        "provider": "local",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": ":memory:"},
    }
    user_profile_source = FileSource(
        name="user_profile_source",
        path=parquet_file_path,
        event_timestamp_column="event_timestamp",
    )

    store = FeatureStore(config=RepoConfig.model_validate(config))
    user_id_entity = Entity(
        name="user_id", value_type=ValueType.INT64, description="User ID"
    )
    user_profile_feature_view = FeatureView(
        name="user_profile",
        entities=[user_id_entity],
        ttl=None,
        schema=[
            Field(name="age", dtype=Int64),
            Field(name="income", dtype=Float64),
        ],
        source=user_profile_source,
    )
    user_feature_service = FeatureService(
        name="user_service",
        features=[user_profile_feature_view],
    )

    # Apply objects
    store.apply([user_id_entity, user_profile_feature_view, user_feature_service])

    # Build REST app with registered routes
    rest_server = RestRegistryServer(store)
    client = TestClient(rest_server.app)

    yield client

    tmp_dir.cleanup()


def test_entities_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/entities?project=demo_project")
    assert response.status_code == 200
    assert "entities" in response.json()
    response = fastapi_test_app.get("/entities/user_id?project=demo_project")
    assert response.status_code == 200
    assert response.json()["spec"]["name"] == "user_id"


def test_feature_views_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/feature_views?project=demo_project")
    assert response.status_code == 200
    assert "featureViews" in response.json()
    response = fastapi_test_app.get("/feature_views/user_profile?project=demo_project")
    assert response.status_code == 200
    assert response.json()["featureView"]["spec"]["name"] == "user_profile"


def test_feature_services_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/feature_services?project=demo_project")
    assert response.status_code == 200
    assert "featureServices" in response.json()
    response = fastapi_test_app.get(
        "/feature_services/user_service?project=demo_project"
    )
    assert response.status_code == 200
    assert response.json()["spec"]["name"] == "user_service"


def test_data_sources_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/data_sources?project=demo_project")
    assert response.status_code == 200
    assert "data_sources" in response.json()
    response = fastapi_test_app.get(
        "/data_sources/user_profile_source?project=demo_project"
    )
    assert response.status_code == 200
    assert response.json()["name"] == "user_profile_source"


def test_projects_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/projects")
    assert response.status_code == 200
    assert isinstance(response.json()["projects"], list)
    response = fastapi_test_app.get("/projects/demo_project")
    assert response.status_code == 200
    assert response.json()["spec"]["name"] == "demo_project"


def test_permissions_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/permissions?project=demo_project")
    assert response.status_code == 200
