import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.api.registry.rest.rest_registry_server import RestRegistryServer
from feast.feature_store import FeatureStore
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset
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

    # Create a saved dataset for testing
    saved_dataset_storage = SavedDatasetFileStorage(path=parquet_file_path)
    test_saved_dataset = SavedDataset(
        name="test_saved_dataset",
        features=["user_profile:age", "user_profile:income"],
        join_keys=["user_id"],
        storage=saved_dataset_storage,
        tags={"environment": "test", "version": "1.0"},
    )

    # Apply objects
    store.apply([user_id_entity, user_profile_feature_view, user_feature_service])
    store._registry.apply_saved_dataset(test_saved_dataset, "demo_project")

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


def test_lineage_registry_via_rest(fastapi_test_app):
    """Test the /lineage/registry endpoint."""
    response = fastapi_test_app.get("/lineage/registry?project=demo_project")
    assert response.status_code == 200

    data = response.json()
    assert "relationships" in data
    assert "indirect_relationships" in data
    assert isinstance(data["relationships"], list)
    assert isinstance(data["indirect_relationships"], list)


def test_lineage_registry_with_filters_via_rest(fastapi_test_app):
    """Test the /lineage/registry endpoint with filters."""
    response = fastapi_test_app.get(
        "/lineage/registry?project=demo_project&filter_object_type=featureView"
    )
    assert response.status_code == 200

    response = fastapi_test_app.get(
        "/lineage/registry?project=demo_project&filter_object_type=featureView&filter_object_name=user_profile"
    )
    assert response.status_code == 200


def test_object_relationships_via_rest(fastapi_test_app):
    """Test the /lineage/objects/{object_type}/{object_name} endpoint."""
    response = fastapi_test_app.get(
        "/lineage/objects/featureView/user_profile?project=demo_project"
    )
    assert response.status_code == 200

    data = response.json()
    assert "relationships" in data
    assert isinstance(data["relationships"], list)


def test_object_relationships_with_indirect_via_rest(fastapi_test_app):
    """Test the object relationships endpoint with indirect relationships."""
    response = fastapi_test_app.get(
        "/lineage/objects/featureView/user_profile?project=demo_project&include_indirect=true"
    )
    assert response.status_code == 200

    data = response.json()
    assert "relationships" in data
    assert isinstance(data["relationships"], list)


def test_object_relationships_invalid_type_via_rest(fastapi_test_app):
    """Test the object relationships endpoint with invalid object type."""
    response = fastapi_test_app.get(
        "/lineage/objects/invalidType/some_name?project=demo_project"
    )
    assert response.status_code == 400

    data = response.json()
    assert "detail" in data
    assert "Invalid object_type" in data["detail"]


def test_complete_registry_data_via_rest(fastapi_test_app):
    """Test the /lineage/complete endpoint."""
    response = fastapi_test_app.get("/lineage/complete?project=demo_project")
    assert response.status_code == 200

    data = response.json()

    assert "project" in data
    assert data["project"] == "demo_project"
    assert "objects" in data
    assert "relationships" in data
    assert "indirectRelationships" in data

    objects = data["objects"]
    assert "entities" in objects
    assert "dataSources" in objects
    assert "featureViews" in objects
    assert "featureServices" in objects

    assert isinstance(objects["entities"], list)
    assert isinstance(objects["dataSources"], list)
    assert isinstance(objects["featureViews"], list)
    assert isinstance(objects["featureServices"], list)


def test_complete_registry_data_cache_control_via_rest(fastapi_test_app):
    """Test the /lineage/complete endpoint with cache control."""
    response = fastapi_test_app.get(
        "/lineage/complete?project=demo_project&allow_cache=false"
    )
    assert response.status_code == 200

    data = response.json()
    assert "project" in data
    response = fastapi_test_app.get(
        "/lineage/complete?project=demo_project&allow_cache=true"
    )
    assert response.status_code == 200


def test_lineage_endpoint_error_handling(fastapi_test_app):
    """Test error handling in lineage endpoints."""
    # Test missing project parameter
    response = fastapi_test_app.get("/lineage/registry")
    assert response.status_code == 422  # Validation error

    # Test invalid project
    response = fastapi_test_app.get("/lineage/registry?project=nonexistent_project")
    # Should still return 200 but with empty results
    assert response.status_code == 200

    # Test object relationships with missing parameters
    response = fastapi_test_app.get("/lineage/objects/featureView/test_fv")
    assert response.status_code == 422  # Missing required project parameter


def test_saved_datasets_via_rest(fastapi_test_app):
    # Test list saved datasets endpoint
    response = fastapi_test_app.get("/saved_datasets?project=demo_project")
    assert response.status_code == 200
    response_data = response.json()
    assert "saved_datasets" in response_data
    assert isinstance(response_data["saved_datasets"], list)
    assert len(response_data["saved_datasets"]) == 1

    saved_dataset = response_data["saved_datasets"][0]
    assert saved_dataset["spec"]["name"] == "test_saved_dataset"
    assert "user_profile:age" in saved_dataset["spec"]["features"]
    assert "user_profile:income" in saved_dataset["spec"]["features"]
    assert "user_id" in saved_dataset["spec"]["joinKeys"]
    assert saved_dataset["spec"]["tags"]["environment"] == "test"
    assert saved_dataset["spec"]["tags"]["version"] == "1.0"

    # Test get specific saved dataset endpoint
    response = fastapi_test_app.get(
        "/saved_datasets/test_saved_dataset?project=demo_project"
    )
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["spec"]["name"] == "test_saved_dataset"
    assert "user_profile:age" in response_data["spec"]["features"]
    assert "user_profile:income" in response_data["spec"]["features"]

    # Test with allow_cache parameter
    response = fastapi_test_app.get(
        "/saved_datasets/test_saved_dataset?project=demo_project&allow_cache=false"
    )
    assert response.status_code == 200
    assert response.json()["spec"]["name"] == "test_saved_dataset"

    # Test with tags filter
    response = fastapi_test_app.get(
        "/saved_datasets?project=demo_project&tags=environment:test"
    )
    assert response.status_code == 200
    assert len(response.json()["saved_datasets"]) == 1

    # Test with non-matching tags filter
    response = fastapi_test_app.get(
        "/saved_datasets?project=demo_project&tags=environment:production"
    )
    assert response.status_code == 200
    assert len(response.json()["saved_datasets"]) == 0

    # Test with multiple tags filter
    response = fastapi_test_app.get(
        "/saved_datasets?project=demo_project&tags=environment:test&tags=version:1.0"
    )
    assert response.status_code == 200
    assert len(response.json()["saved_datasets"]) == 1

    # Test non-existent saved dataset
    response = fastapi_test_app.get("/saved_datasets/non_existent?project=demo_project")
    assert response.status_code == 404

    # Test missing project parameter
    response = fastapi_test_app.get("/saved_datasets/test_saved_dataset")
    assert (
        response.status_code == 422
    )  # Unprocessable Entity for missing required query param
