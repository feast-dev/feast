import ast
import os
import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from feast import Entity, FeatureService, FeatureStore, FeatureView, Field, FileSource
from feast.api.registry.rest.rest_registry_server import RestRegistryServer
from feast.data_source import RequestSource
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.on_demand_feature_view import on_demand_feature_view
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
        tags={"environment": "production", "team": "ml", "version": "1.0"},
    )
    user_behavior_feature_view = FeatureView(
        name="user_behavior",
        entities=[user_id_entity],
        ttl=None,
        schema=[
            Field(name="click_count", dtype=Int64),
            Field(name="session_duration", dtype=Float64),
        ],
        source=user_profile_source,
        tags={"environment": "staging", "team": "analytics", "version": "2.0"},
    )

    user_preferences_feature_view = FeatureView(
        name="user_preferences",
        entities=[user_id_entity],
        ttl=None,
        schema=[
            Field(name="preferred_category", dtype=Int64),
            Field(name="engagement_score", dtype=Float64),
        ],
        source=user_profile_source,
        tags={"environment": "production", "team": "analytics", "version": "1.5"},
    )

    user_feature_service = FeatureService(
        name="user_service",
        features=[
            user_profile_feature_view,
            user_behavior_feature_view,
            user_preferences_feature_view,
        ],
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
    input_request = RequestSource(
        name="input_request_source",
        schema=[
            Field(name="request_feature", dtype=Float64),
        ],
    )

    @on_demand_feature_view(
        sources=[user_profile_feature_view, input_request],
        schema=[
            Field(name="combined_feature", dtype=Float64),
        ],
        description="On-demand feature view with request source for testing",
    )
    def test_on_demand_feature_view(features_df: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        df["combined_feature"] = features_df["age"] + features_df["request_feature"]
        return df

    # Apply objects
    store.apply(
        [
            user_id_entity,
            user_profile_feature_view,
            user_behavior_feature_view,
            user_preferences_feature_view,
            user_feature_service,
            test_on_demand_feature_view,
        ]
    )
    store.registry.apply_saved_dataset(test_saved_dataset, "demo_project")

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
    data = response.json()
    assert data["spec"]["name"] == "user_id"
    # Check featureDefinition
    assert "featureDefinition" in data
    code = data["featureDefinition"]
    assert code
    assert "Entity" in code
    assert "user_id" in code
    try:
        ast.parse(code)
    except SyntaxError as e:
        pytest.fail(f"featureDefinition is not valid Python: {e}")


def test_feature_views_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/feature_views?project=demo_project")
    assert response.status_code == 200
    assert "featureViews" in response.json()
    response = fastapi_test_app.get("/feature_views/user_profile?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert data["spec"]["name"] == "user_profile"
    # Check featureDefinition
    assert "featureDefinition" in data
    code = data["featureDefinition"]
    assert code
    assert "FeatureView" in code
    assert "user_profile" in code
    try:
        ast.parse(code)
    except SyntaxError as e:
        pytest.fail(f"featureDefinition is not valid Python: {e}")


def test_feature_views_type_field_via_rest(fastapi_test_app):
    """Test that the type field is correctly populated for feature views."""
    # Test list endpoint
    response = fastapi_test_app.get("/feature_views?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data

    # Verify all feature views have a type field
    for fv in data["featureViews"]:
        assert "type" in fv
        assert fv["type"] is not None
        assert fv["type"] in ["featureView", "onDemandFeatureView"]

    # Test single endpoint
    response = fastapi_test_app.get("/feature_views/user_profile?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "type" in data
    assert data["type"] == "featureView"
    assert data["spec"]["name"] == "user_profile"


def test_feature_views_entity_filtering_via_rest(fastapi_test_app):
    """Test that feature views can be filtered by entity."""
    response = fastapi_test_app.get("/feature_views?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    all_feature_views = data["featureViews"]

    response = fastapi_test_app.get("/feature_views?project=demo_project&entity=user")
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    filtered_feature_views = data["featureViews"]

    assert len(filtered_feature_views) <= len(all_feature_views)

    for fv in filtered_feature_views:
        if "spec" in fv and "entities" in fv["spec"]:
            assert "user" in fv["spec"]["entities"]

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&entity=nonexistent_entity"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    assert len(data["featureViews"]) == 0


def test_feature_views_comprehensive_filtering_via_rest(fastapi_test_app):
    """Test that feature views can be filtered by multiple criteria."""
    response = fastapi_test_app.get("/feature_views?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    all_feature_views = data["featureViews"]

    response = fastapi_test_app.get("/feature_views?project=demo_project&feature=age")
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    feature_filtered_views = data["featureViews"]
    assert len(feature_filtered_views) <= len(all_feature_views)

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&data_source=user_profile_source"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    data_source_filtered_views = data["featureViews"]
    assert len(data_source_filtered_views) <= len(all_feature_views)

    # Test filtering on-demand feature views by request source data source
    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&data_source=input_request_source"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    odfv_data_source_filtered_views = data["featureViews"]

    # Should find the on-demand feature view that uses the request source
    assert len(odfv_data_source_filtered_views) > 0
    odfv_found = False
    for fv in odfv_data_source_filtered_views:
        if fv["type"] == "onDemandFeatureView":
            odfv_found = True
            break
    assert odfv_found, (
        "On-demand feature view should be found when filtering by request source data source"
    )

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&feature_service=user_service"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    feature_service_filtered_views = data["featureViews"]
    assert len(feature_service_filtered_views) <= len(all_feature_views)

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&entity=user&feature=age"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    combined_filtered_views = data["featureViews"]
    assert len(combined_filtered_views) <= len(all_feature_views)

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&feature=nonexistent_feature"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    assert len(data["featureViews"]) == 0

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&data_source=nonexistent_source"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    assert len(data["featureViews"]) == 0

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&feature_service=nonexistent_service"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    assert len(data["featureViews"]) == 0

    response = fastapi_test_app.get(
        "/feature_views?project=demo_project&feature_service=restricted_feature_service"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    assert len(data["featureViews"]) == 0


def test_feature_services_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/feature_services?project=demo_project")
    assert response.status_code == 200
    assert "featureServices" in response.json()
    response = fastapi_test_app.get(
        "/feature_services/user_service?project=demo_project"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["spec"]["name"] == "user_service"
    # Check featureDefinition
    assert "featureDefinition" in data
    code = data["featureDefinition"]
    assert code
    assert "FeatureService" in code
    assert "user_service" in code
    try:
        ast.parse(code)
    except SyntaxError as e:
        pytest.fail(f"featureDefinition is not valid Python: {e}")


def test_feature_services_feature_view_filtering_via_rest(fastapi_test_app):
    """Test that feature services can be filtered by feature view name."""
    response = fastapi_test_app.get("/feature_services?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "featureServices" in data
    all_feature_services = data["featureServices"]

    response = fastapi_test_app.get(
        "/feature_services?project=demo_project&feature_view=user_profile"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureServices" in data
    filtered_feature_services = data["featureServices"]

    assert len(filtered_feature_services) <= len(all_feature_services)

    for fs in filtered_feature_services:
        if "spec" in fs and "featureViewProjections" in fs["spec"]:
            feature_view_names = [
                fvp["name"] for fvp in fs["spec"]["featureViewProjections"]
            ]
            assert "user_profile" in feature_view_names

    response = fastapi_test_app.get(
        "/feature_services?project=demo_project&feature_view=nonexistent_feature_view"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureServices" in data
    assert len(data["featureServices"]) == 0


def test_data_sources_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/data_sources?project=demo_project")
    assert "dataSources" in response.json()
    response = fastapi_test_app.get(
        "/data_sources/user_profile_source?project=demo_project"
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "user_profile_source"
    # Check featureDefinition
    assert "featureDefinition" in data
    code = data["featureDefinition"]
    assert code
    assert "FileSource" in code
    assert "user_profile_source" in code
    try:
        ast.parse(code)
    except SyntaxError as e:
        pytest.fail(f"featureDefinition is not valid Python: {e}")


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
    assert response.status_code == 422

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "Invalid object_type" in data["detail"]
    assert "error_type" in data
    assert data["error_type"] == "ValueError"


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

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"

    # Test invalid project
    response = fastapi_test_app.get("/lineage/registry?project=nonexistent_project")
    # Should still return 200 but with empty results
    assert response.status_code == 200

    # Test object relationships with missing parameters
    response = fastapi_test_app.get("/lineage/objects/featureView/test_fv")
    assert response.status_code == 422  # Missing required project parameter

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"


def test_saved_datasets_via_rest(fastapi_test_app):
    # Test list saved datasets endpoint
    response = fastapi_test_app.get("/saved_datasets?project=demo_project")
    assert response.status_code == 200
    response_data = response.json()
    assert "savedDatasets" in response_data
    assert isinstance(response_data["savedDatasets"], list)
    assert len(response_data["savedDatasets"]) == 1

    saved_dataset = response_data["savedDatasets"][0]
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
    assert len(response.json()["savedDatasets"]) == 1

    # Test with non-matching tags filter
    response = fastapi_test_app.get(
        "/saved_datasets?project=demo_project&tags=environment:production"
    )
    assert response.status_code == 200
    assert len(response.json()["savedDatasets"]) == 0

    # Test with multiple tags filter
    response = fastapi_test_app.get(
        "/saved_datasets?project=demo_project&tags=environment:test&tags=version:1.0"
    )
    assert response.status_code == 200
    assert len(response.json()["savedDatasets"]) == 1

    # Test non-existent saved dataset
    response = fastapi_test_app.get("/saved_datasets/non_existent?project=demo_project")
    assert response.status_code == 404

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 404
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test missing project parameter
    response = fastapi_test_app.get("/saved_datasets/test_saved_dataset")
    assert (
        response.status_code == 422
    )  # Unprocessable Entity for missing required query param

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"


@pytest.fixture
def fastapi_test_app_with_multiple_objects():
    """Test app with multiple objects for pagination and sorting tests."""
    tmp_dir = tempfile.TemporaryDirectory()
    registry_path = os.path.join(tmp_dir.name, "registry.db")

    parquet_file_path = os.path.join(tmp_dir.name, "data.parquet")

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
    config = {
        "registry": registry_path,
        "project": "demo_project",
        "provider": "local",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": ":memory:"},
    }

    store = FeatureStore(config=RepoConfig.model_validate(config))

    # Create multiple entities for testing
    entities = [
        Entity(name="user_id", value_type=ValueType.INT64, description="User ID"),
        Entity(
            name="customer_id", value_type=ValueType.INT64, description="Customer ID"
        ),
        Entity(name="product_id", value_type=ValueType.INT64, description="Product ID"),
        Entity(name="order_id", value_type=ValueType.INT64, description="Order ID"),
        Entity(name="session_id", value_type=ValueType.INT64, description="Session ID"),
    ]

    data_sources = [
        FileSource(
            name="user_profile_source",
            path=parquet_file_path,
            event_timestamp_column="event_timestamp",
        ),
        FileSource(
            name="customer_data_source",
            path=parquet_file_path,
            event_timestamp_column="event_timestamp",
        ),
        FileSource(
            name="product_catalog_source",
            path=parquet_file_path,
            event_timestamp_column="event_timestamp",
        ),
    ]

    feature_views = [
        FeatureView(
            name="user_profile",
            entities=[entities[0]],
            ttl=None,
            schema=[
                Field(name="age", dtype=Int64),
                Field(name="income", dtype=Float64),
            ],
            source=data_sources[0],
        ),
        FeatureView(
            name="customer_features",
            entities=[entities[1]],
            ttl=None,
            schema=[
                Field(name="age", dtype=Int64),
            ],
            source=data_sources[1],
        ),
        FeatureView(
            name="product_features",
            entities=[entities[2]],
            ttl=None,
            schema=[
                Field(name="income", dtype=Float64),
            ],
            source=data_sources[2],
        ),
    ]

    feature_services = [
        FeatureService(
            name="user_service",
            features=[feature_views[0]],
        ),
        FeatureService(
            name="customer_service",
            features=[feature_views[1]],
        ),
        FeatureService(
            name="analytics_service",
            features=[feature_views[0], feature_views[1]],
        ),
    ]

    saved_datasets = [
        SavedDataset(
            name="dataset_alpha",
            features=["user_profile:age"],
            join_keys=["user_id"],
            storage=SavedDatasetFileStorage(path=parquet_file_path),
            tags={"environment": "test", "version": "1.0"},
        ),
        SavedDataset(
            name="dataset_beta",
            features=["user_profile:income"],
            join_keys=["user_id"],
            storage=SavedDatasetFileStorage(path=parquet_file_path),
            tags={"environment": "prod", "version": "2.0"},
        ),
        SavedDataset(
            name="dataset_gamma",
            features=["customer_features:age"],
            join_keys=["customer_id"],
            storage=SavedDatasetFileStorage(path=parquet_file_path),
            tags={"environment": "test", "version": "1.5"},
        ),
    ]
    store.apply(entities + data_sources + feature_views + feature_services)

    for dataset in saved_datasets:
        store.registry.apply_saved_dataset(dataset, "demo_project")

    rest_server = RestRegistryServer(store)
    client = TestClient(rest_server.app)

    yield client

    tmp_dir.cleanup()


def test_entities_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for entities endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test basic pagination - first page
    response = client.get("/entities?project=demo_project&page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert "entities" in data
    assert "pagination" in data
    assert len(data["entities"]) == 2
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2
    assert data["pagination"]["totalCount"] == 6
    assert data["pagination"]["totalPages"] == 3
    assert data["pagination"]["hasNext"] is True

    # Test pagination - second page
    response = client.get("/entities?project=demo_project&page=2&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["entities"]) == 2
    assert data["pagination"]["page"] == 2
    assert data["pagination"]["hasNext"] is True

    # Test pagination - last page
    response = client.get("/entities?project=demo_project&page=3&limit=2")
    assert response.status_code == 200
    data = response.json()
    # Page 3 might be beyond available pages
    assert data["pagination"]["page"] == 3

    # Test pagination beyond available pages
    response = client.get("/entities?project=demo_project&page=5&limit=2")
    assert response.status_code == 200
    data = response.json()
    # Beyond available pages should not include entities key
    assert "entities" not in data or len(data["entities"]) == 0
    assert data["pagination"]["page"] == 5


def test_entities_sorting_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting for entities endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test sorting by name ascending
    response = client.get("/entities?project=demo_project&sort_by=name&sort_order=asc")
    assert response.status_code == 200
    data = response.json()
    entity_names = [entity["spec"]["name"] for entity in data["entities"]]
    assert entity_names == sorted(entity_names)

    # Test sorting by name descending
    response = client.get("/entities?project=demo_project&sort_by=name&sort_order=desc")
    assert response.status_code == 200
    data = response.json()
    entity_names = [entity["spec"]["name"] for entity in data["entities"]]
    assert entity_names == sorted(entity_names, reverse=True)


def test_entities_pagination_with_sorting_via_rest(
    fastapi_test_app_with_multiple_objects,
):
    """Test combined pagination and sorting for entities endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test pagination with sorting
    response = client.get(
        "/entities?project=demo_project&page=1&limit=2&sort_by=name&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["entities"]) == 2
    entity_names = [entity["spec"]["name"] for entity in data["entities"]]
    assert entity_names == sorted(entity_names)
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2
    assert data["pagination"]["totalCount"] == 6


def test_feature_views_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for feature views endpoint."""
    client = fastapi_test_app_with_multiple_objects

    response = client.get("/feature_views?project=demo_project&page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    assert "pagination" in data
    assert len(data["featureViews"]) == 2
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2
    assert data["pagination"]["totalCount"] == 3
    assert data["pagination"]["totalPages"] == 2
    assert data["pagination"]["hasNext"] is True


def test_feature_views_sorting_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting for feature views endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test sorting by name ascending
    response = client.get(
        "/feature_views?project=demo_project&sort_by=name&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    fv_names = [fv["spec"]["name"] for fv in data["featureViews"]]
    assert fv_names == sorted(fv_names)

    # Test sorting by name descending
    response = client.get(
        "/feature_views?project=demo_project&sort_by=name&sort_order=desc"
    )
    assert response.status_code == 200
    data = response.json()
    fv_names = [fv["spec"]["name"] for fv in data["featureViews"]]
    assert fv_names == sorted(fv_names, reverse=True)


def test_feature_services_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for feature services endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test basic pagination
    response = client.get("/feature_services?project=demo_project&page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert "featureServices" in data
    assert "pagination" in data
    assert len(data["featureServices"]) == 2
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2
    assert data["pagination"]["totalCount"] == 3
    assert data["pagination"]["totalPages"] == 2


def test_feature_services_sorting_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting for feature services endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test sorting by name ascending
    response = client.get(
        "/feature_services?project=demo_project&sort_by=name&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    fs_names = [fs["spec"]["name"] for fs in data["featureServices"]]
    assert fs_names == sorted(fs_names)


def test_data_sources_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for data sources endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test basic pagination
    response = client.get("/data_sources?project=demo_project&page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert "dataSources" in data
    assert "pagination" in data
    assert len(data["dataSources"]) == 2
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2
    assert data["pagination"]["totalCount"] == 3
    assert data["pagination"]["totalPages"] == 2


def test_data_sources_sorting_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting for data sources endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test sorting by name ascending
    response = client.get(
        "/data_sources?project=demo_project&sort_by=name&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    ds_names = [ds["name"] for ds in data["dataSources"]]
    assert ds_names == sorted(ds_names)


def test_saved_datasets_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for saved datasets endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test basic pagination
    response = client.get("/saved_datasets?project=demo_project&page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert "savedDatasets" in data
    assert "pagination" in data
    assert len(data["savedDatasets"]) == 2
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2
    assert data["pagination"]["totalCount"] == 3
    assert data["pagination"]["totalPages"] == 2


def test_saved_datasets_sorting_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting for saved datasets endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test sorting by name ascending
    response = client.get(
        "/saved_datasets?project=demo_project&sort_by=name&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    sd_names = [sd["spec"]["name"] for sd in data["savedDatasets"]]
    assert sd_names == sorted(sd_names)

    # Test sorting by name descending
    response = client.get(
        "/saved_datasets?project=demo_project&sort_by=name&sort_order=desc"
    )
    assert response.status_code == 200
    data = response.json()
    sd_names = [sd["spec"]["name"] for sd in data["savedDatasets"]]
    assert sd_names == sorted(sd_names, reverse=True)


def test_projects_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for projects endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test basic pagination
    response = client.get("/projects?page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert "projects" in data
    assert "pagination" in data
    # Should have at least 1 project (demo_project)
    assert len(data["projects"]) >= 1
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2


def test_projects_sorting_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting for projects endpoint."""
    client = fastapi_test_app_with_multiple_objects

    # Test sorting by name ascending
    response = client.get("/projects?sort_by=name&sort_order=asc")
    assert response.status_code == 200
    data = response.json()
    project_names = [project["spec"]["name"] for project in data["projects"]]
    assert project_names == sorted(project_names)


def test_pagination_invalid_parameters_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination with invalid parameters."""
    client = fastapi_test_app_with_multiple_objects

    # Test invalid page number (negative)
    response = client.get("/entities?project=demo_project&page=-1&limit=2")
    assert response.status_code == 422  # Validation error

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"

    # Test invalid limit (negative)
    response = client.get("/entities?project=demo_project&page=1&limit=-1")
    assert response.status_code == 422  # Validation error

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"

    # Test invalid limit (too large)
    response = client.get("/entities?project=demo_project&page=1&limit=1000")
    assert response.status_code == 422  # Validation error

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"

    # Test invalid page number (zero)
    response = client.get("/entities?project=demo_project&page=0&limit=2")
    assert response.status_code == 422  # Validation error

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"


def test_sorting_invalid_parameters_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting with invalid parameters."""
    client = fastapi_test_app_with_multiple_objects

    # Test invalid sort_order
    response = client.get(
        "/entities?project=demo_project&sort_by=name&sort_order=invalid"
    )
    assert response.status_code == 422  # Validation error

    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 422
    assert "detail" in data
    assert "error_type" in data
    assert data["error_type"] == "RequestValidationError"

    # Test with only sort_by (should default to asc)
    response = client.get("/entities?project=demo_project&sort_by=name")
    assert response.status_code == 200
    data = response.json()
    entity_names = [entity["spec"]["name"] for entity in data["entities"]]
    assert entity_names == sorted(entity_names)


def test_pagination_no_parameters_via_rest(fastapi_test_app_with_multiple_objects):
    """Test that endpoints work without pagination parameters."""
    client = fastapi_test_app_with_multiple_objects

    # Test entities without pagination
    response = client.get("/entities?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "entities" in data
    assert "pagination" in data
    # All entities should be returned (includes the dummy entity)
    assert len(data["entities"]) == 6
    assert data["pagination"]["totalCount"] == 6
    assert data["pagination"]["totalPages"] == 1


def test_lineage_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for lineage endpoints."""
    client = fastapi_test_app_with_multiple_objects

    # Test lineage registry endpoint with pagination
    response = client.get("/lineage/registry?project=demo_project&page=1&limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data
    assert "indirect_relationships" in data
    assert "relationships_pagination" in data
    assert "indirect_relationships_pagination" in data

    # Test object relationships endpoint with pagination
    response = client.get(
        "/lineage/objects/featureView/user_profile?project=demo_project&page=1&limit=5"
    )
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data
    assert "pagination" in data


def test_lineage_sorting_via_rest(fastapi_test_app_with_multiple_objects):
    """Test sorting for lineage endpoints."""
    client = fastapi_test_app_with_multiple_objects

    # Test lineage registry endpoint with sorting
    response = client.get(
        "/lineage/registry?project=demo_project&sort_by=id&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data
    assert "indirect_relationships" in data

    # Test object relationships endpoint with sorting
    response = client.get(
        "/lineage/objects/featureView/user_profile?project=demo_project&sort_by=id&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data


def test_features_list_via_rest(fastapi_test_app):
    """Test the /features endpoint (list features in a project)."""
    response = fastapi_test_app.get("/features?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "features" in data
    assert "pagination" in data
    for feature in data["features"]:
        assert "name" in feature
        assert "featureView" in feature
        assert "type" in feature

    pagination = data["pagination"]
    assert "totalCount" in pagination
    assert "totalPages" in pagination


def test_features_list_with_relationships_via_rest(fastapi_test_app):
    """Test the /features endpoint with include_relationships."""
    response = fastapi_test_app.get(
        "/features?project=demo_project&include_relationships=true"
    )
    assert response.status_code == 200
    data = response.json()
    assert "features" in data
    assert "relationships" in data
    assert isinstance(data["relationships"], dict)
    for k, v in data["relationships"].items():
        assert isinstance(v, list)
        for rel in v:
            assert "source" in rel and "target" in rel


def test_features_get_invalid_feature_view_via_rest(fastapi_test_app):
    """Test the /features/{feature_view}/{name} endpoint with invalid feature_view."""
    response = fastapi_test_app.get(
        "/features/invalid_fv/invalid_feature?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert "status_code" in data
    assert data["status_code"] == 404
    assert "detail" in data
    assert "not found" in data["detail"].lower()
    assert "error_type" in data
    assert data["error_type"] == "FeastObjectNotFoundException"


def test_features_get_via_rest(fastapi_test_app):
    """Test the /features/{feature_view}/{name} endpoint (get single feature)."""
    response = fastapi_test_app.get("/features/user_profile/age?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "age"
    assert data["featureView"] == "user_profile"
    assert data["type"] == "Int64"
    # Check featureDefinition
    assert "featureDefinition" in data
    code = data["featureDefinition"]
    assert code
    assert "Feature" in code
    assert "age" in code
    try:
        ast.parse(code)
    except SyntaxError as e:
        pytest.fail(f"featureDefinition is not valid Python: {e}")

    response = fastapi_test_app.get(
        "/features/user_profile/age?project=demo_project&include_relationships=true"
    )
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data
    assert isinstance(data["relationships"], list)
    for rel in data["relationships"]:
        assert "source" in rel and "target" in rel


def test_features_list_all_via_rest(fastapi_test_app):
    """Test the /features/all endpoint (all projects)."""
    response = fastapi_test_app.get("/features/all")
    assert response.status_code == 200
    data = response.json()
    assert "features" in data
    assert "pagination" in data
    for feature in data["features"]:
        assert "project" in feature
        assert "name" in feature
        assert "featureView" in feature
        assert "type" in feature


def test_features_filtering_and_sorting_via_rest(fastapi_test_app):
    """Test filtering and sorting for /features endpoint."""
    response = fastapi_test_app.get(
        "/features?project=demo_project&feature_view=user_profile"
    )
    assert response.status_code == 200
    data = response.json()
    for feature in data["features"]:
        assert feature["featureView"] == "user_profile"

    response = fastapi_test_app.get("/features?project=demo_project&name=age")
    assert response.status_code == 200
    data = response.json()
    for feature in data["features"]:
        assert feature["name"] == "age"

    response = fastapi_test_app.get(
        "/features?project=demo_project&sort_by=name&sort_order=asc"
    )
    assert response.status_code == 200
    data = response.json()
    names = [f["name"] for f in data["features"]]
    assert names == sorted(names)


def test_features_pagination_via_rest(fastapi_test_app_with_multiple_objects):
    """Test pagination for /features endpoint."""
    client = fastapi_test_app_with_multiple_objects
    response = client.get("/features?project=demo_project&page=1&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert "features" in data
    assert "pagination" in data
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["limit"] == 2

    response = client.get("/features?project=demo_project&page=2&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert data["pagination"]["page"] == 2


def test_lineage_features_object_type_via_rest(fastapi_test_app):
    """Test lineage endpoints for features as a first-class object."""
    response = fastapi_test_app.get("/lineage/objects/feature/age?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data
    assert isinstance(data["relationships"], list)
    response = fastapi_test_app.get(
        "/lineage/registry?project=demo_project&filter_object_type=feature"
    )
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data
    assert isinstance(data["relationships"], list)


def test_feature_view_type_identification():
    """Test that we can properly identify feature view types from their structure."""
    from feast.api.registry.rest.feature_views import _extract_feature_view_from_any

    any_feature_view_1 = {"featureView": {"spec": {"name": "test_fv"}, "meta": {}}}
    any_feature_view_2 = {
        "onDemandFeatureView": {"spec": {"name": "test_odfv"}, "meta": {}}
    }
    any_feature_view_3 = {
        "streamFeatureView": {"spec": {"name": "test_sfv"}, "meta": {}}
    }

    result_1 = _extract_feature_view_from_any(any_feature_view_1)
    result_2 = _extract_feature_view_from_any(any_feature_view_2)
    result_3 = _extract_feature_view_from_any(any_feature_view_3)

    assert result_1["type"] == "featureView"
    assert result_2["type"] == "onDemandFeatureView"
    assert result_3["type"] == "streamFeatureView"


def test_entities_all_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/entities/all")
    assert response.status_code == 200
    data = response.json()
    assert "entities" in data
    for entity in data["entities"]:
        assert "project" in entity
        assert entity["project"] == "demo_project"


def test_feature_views_all_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/feature_views/all")
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    for fv in data["featureViews"]:
        assert "project" in fv
        assert fv["project"] == "demo_project"


def test_data_sources_all_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/data_sources/all")
    assert response.status_code == 200
    data = response.json()
    assert "dataSources" in data
    for ds in data["dataSources"]:
        assert "project" in ds
        assert ds["project"] == "demo_project"


def test_feature_services_all_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/feature_services/all")
    assert response.status_code == 200
    data = response.json()
    assert "featureServices" in data
    for fs in data["featureServices"]:
        assert "project" in fs
        assert fs["project"] == "demo_project"


def test_saved_datasets_all_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/saved_datasets/all")
    assert response.status_code == 200
    data = response.json()
    assert "savedDatasets" in data
    for sd in data["savedDatasets"]:
        assert "project" in sd
        assert sd["project"] == "demo_project"


def test_lineage_registry_all_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/lineage/registry/all")
    assert response.status_code == 200
    data = response.json()
    assert "relationships" in data
    assert "indirect_relationships" in data
    for rel in data["relationships"]:
        assert "project" in rel
        assert rel["project"] == "demo_project"
    for rel in data["indirect_relationships"]:
        assert "project" in rel
        assert rel["project"] == "demo_project"


def test_lineage_complete_all_via_rest(fastapi_test_app):
    response = fastapi_test_app.get("/lineage/complete/all")
    assert response.status_code == 200
    data = response.json()
    assert "projects" in data
    for project_data in data["projects"]:
        assert "project" in project_data
        assert project_data["project"] == "demo_project"
        assert "objects" in project_data
        assert "entities" in project_data["objects"]
        assert "dataSources" in project_data["objects"]
        assert "featureViews" in project_data["objects"]
        assert "featureServices" in project_data["objects"]


def test_invalid_project_name_with_relationships_via_rest(fastapi_test_app):
    """Test REST API response with invalid project name using include_relationships=true.
    The API should not throw 500 or any other error when an invalid project name is provided
    with include_relationships=true parameter.
    """
    response = fastapi_test_app.get(
        "/entities?project=invalid_project_name&include_relationships=true"
    )
    assert response.status_code == 200
    data = response.json()
    assert "entities" in data
    assert isinstance(data["entities"], list)
    assert len(data["entities"]) == 0
    assert "relationships" in data
    assert isinstance(data["relationships"], dict)
    assert len(data["relationships"]) == 0

    response = fastapi_test_app.get(
        "/feature_views?project=invalid_project_name&include_relationships=true"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureViews" in data
    assert isinstance(data["featureViews"], list)
    assert len(data["featureViews"]) == 0
    assert "relationships" in data
    assert isinstance(data["relationships"], dict)
    assert len(data["relationships"]) == 0

    response = fastapi_test_app.get(
        "/data_sources?project=invalid_project_name&include_relationships=true"
    )
    # Should return 200 with empty results, not 500 or other errors
    assert response.status_code == 200
    data = response.json()
    assert "dataSources" in data
    assert isinstance(data["dataSources"], list)
    assert len(data["dataSources"]) == 0
    assert "relationships" in data
    assert isinstance(data["relationships"], dict)
    assert len(data["relationships"]) == 0

    response = fastapi_test_app.get(
        "/feature_services?project=invalid_project_name&include_relationships=true"
    )
    assert response.status_code == 200
    data = response.json()
    assert "featureServices" in data
    assert isinstance(data["featureServices"], list)
    assert len(data["featureServices"]) == 0
    assert "relationships" in data
    assert isinstance(data["relationships"], dict)
    assert len(data["relationships"]) == 0

    response = fastapi_test_app.get(
        "/features?project=invalid_project_name&include_relationships=true"
    )
    assert response.status_code == 200
    data = response.json()
    assert "features" in data
    assert isinstance(data["features"], list)
    assert len(data["features"]) == 0
    assert "relationships" in data
    assert isinstance(data["relationships"], dict)
    assert len(data["relationships"]) == 0


def test_metrics_resource_counts_via_rest(fastapi_test_app):
    """Test the /metrics/resource_counts endpoint."""
    # Test with specific project
    response = fastapi_test_app.get("/metrics/resource_counts?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "project" in data
    assert data["project"] == "demo_project"
    assert "counts" in data

    counts = data["counts"]
    assert "entities" in counts
    assert "dataSources" in counts
    assert "savedDatasets" in counts
    assert "features" in counts
    assert "featureViews" in counts
    assert "featureServices" in counts

    # Verify counts are integers
    for key, value in counts.items():
        assert isinstance(value, int)
        assert value >= 0

    # Test without project parameter (should return all projects)
    response = fastapi_test_app.get("/metrics/resource_counts")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "perProject" in data

    total = data["total"]
    assert "entities" in total
    assert "dataSources" in total
    assert "savedDatasets" in total
    assert "features" in total
    assert "featureViews" in total
    assert "featureServices" in total

    per_project = data["perProject"]
    assert "demo_project" in per_project
    assert isinstance(per_project["demo_project"], dict)


def test_feature_views_all_types_and_resource_counts_match(fastapi_test_app):
    """
    Test that verifies:
    1. All types of feature views (regular, on-demand, stream) are returned in /feature_views/all
    2. The count from /metrics/resource_counts matches the count from /feature_views/all
    """
    response_all = fastapi_test_app.get("/feature_views/all")
    assert response_all.status_code == 200
    data_all = response_all.json()
    assert "featureViews" in data_all

    feature_views = data_all["featureViews"]

    # Count should include at least:
    # - 3 regular feature views: user_profile, user_behavior, user_preferences
    # - 1 on-demand feature view: test_on_demand_feature_view
    assert len(feature_views) >= 4, (
        f"Expected at least 4 feature views, got {len(feature_views)}"
    )

    # Verify we have different types of feature views
    feature_view_names = {fv["spec"]["name"] for fv in feature_views}

    # Check for regular feature views
    assert "user_profile" in feature_view_names, (
        "Regular feature view 'user_profile' not found"
    )
    assert "user_behavior" in feature_view_names, (
        "Regular feature view 'user_behavior' not found"
    )
    assert "user_preferences" in feature_view_names, (
        "Regular feature view 'user_preferences' not found"
    )

    # Check for on-demand feature view
    assert "test_on_demand_feature_view" in feature_view_names, (
        "On-demand feature view 'test_on_demand_feature_view' not found"
    )

    # Verify all have the correct project
    for fv in feature_views:
        assert fv["project"] == "demo_project", (
            f"Feature view has incorrect project: {fv.get('project')}"
        )

    # Now get resource counts from /metrics/resource_counts endpoint
    response_metrics = fastapi_test_app.get(
        "/metrics/resource_counts?project=demo_project"
    )
    assert response_metrics.status_code == 200
    data_metrics = response_metrics.json()
    assert "counts" in data_metrics

    counts = data_metrics["counts"]
    assert "featureViews" in counts

    # Verify that the count from metrics matches the count from feature_views/all
    feature_views_count_from_all = len(feature_views)
    feature_views_count_from_metrics = counts["featureViews"]

    assert feature_views_count_from_all == feature_views_count_from_metrics, (
        f"Feature views count mismatch: /feature_views/all returned {feature_views_count_from_all} "
        f"but /metrics/resource_counts returned {feature_views_count_from_metrics}"
    )

    # Test without project parameter (all projects)
    response_all_projects = fastapi_test_app.get("/feature_views/all")
    assert response_all_projects.status_code == 200
    data_all_projects = response_all_projects.json()

    response_metrics_all = fastapi_test_app.get("/metrics/resource_counts")
    assert response_metrics_all.status_code == 200
    data_metrics_all = response_metrics_all.json()

    total_fv_count_from_all = len(data_all_projects["featureViews"])
    total_fv_count_from_metrics = data_metrics_all["total"]["featureViews"]

    assert total_fv_count_from_all == total_fv_count_from_metrics, (
        f"Total feature views count mismatch across all projects: "
        f"/feature_views/all returned {total_fv_count_from_all} "
        f"but /metrics/resource_counts returned {total_fv_count_from_metrics}"
    )


def test_metrics_recently_visited_via_rest(fastapi_test_app):
    """Test the /metrics/recently_visited endpoint."""
    # First, make some requests to generate visit data
    fastapi_test_app.get("/entities?project=demo_project")
    fastapi_test_app.get("/entities/user_id?project=demo_project")
    fastapi_test_app.get("/feature_services?project=demo_project")

    # Test basic recently visited endpoint
    response = fastapi_test_app.get("/metrics/recently_visited?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    assert "visits" in data
    assert "pagination" in data

    visits = data["visits"]
    assert isinstance(visits, list)

    # Check visit structure
    if visits:
        visit = visits[0]
        assert "path" in visit
        assert "timestamp" in visit
        assert "project" in visit
        assert "user" in visit
        assert "object" in visit
        assert "object_name" in visit
        assert "method" in visit

        # Verify timestamp format
        import datetime

        datetime.datetime.fromisoformat(visit["timestamp"].replace("Z", "+00:00"))

    pagination = data["pagination"]
    assert "totalCount" in pagination
    assert isinstance(pagination["totalCount"], int)


def test_metrics_recently_visited_with_object_filter(fastapi_test_app):
    """Test filtering by object type for recently visited endpoint."""
    # Generate visit data for different object types
    fastapi_test_app.get("/entities?project=demo_project")
    fastapi_test_app.get("/feature_services?project=demo_project")
    fastapi_test_app.get("/data_sources?project=demo_project")

    # Test filtering by entities only
    response = fastapi_test_app.get(
        "/metrics/recently_visited?project=demo_project&object=entities"
    )
    assert response.status_code == 200
    data = response.json()
    assert "visits" in data

    visits = data["visits"]
    for visit in visits:
        assert visit["object"] == "entities"

    # Test filtering by feature_services
    response = fastapi_test_app.get(
        "/metrics/recently_visited?project=demo_project&object=feature_services"
    )
    assert response.status_code == 200
    data = response.json()
    visits = data["visits"]
    for visit in visits:
        assert visit["object"] == "feature_services"


def test_metrics_recently_visited_error_handling(fastapi_test_app):
    """Test error handling for recently visited endpoint."""
    # Test with non-existent project
    response = fastapi_test_app.get(
        "/metrics/recently_visited?project=nonexistent_project"
    )
    assert response.status_code == 200
    data = response.json()
    assert "visits" in data
    assert len(data["visits"]) == 0

    # Test with invalid object type
    response = fastapi_test_app.get(
        "/metrics/recently_visited?project=demo_project&object=invalid_type"
    )
    assert response.status_code == 200
    data = response.json()
    assert "visits" in data
    assert len(data["visits"]) == 0


def test_metrics_recently_visited_user_isolation(fastapi_test_app):
    """Test that visits are isolated per user."""
    # Make requests as "anonymous" user (default)
    fastapi_test_app.get("/entities?project=demo_project")

    # Check that visits are recorded for anonymous user
    response = fastapi_test_app.get("/metrics/recently_visited?project=demo_project")
    assert response.status_code == 200
    data = response.json()
    visits = data["visits"]

    # All visits should be for anonymous user
    for visit in visits:
        assert visit["user"] == "anonymous"


def test_metrics_popular_tags_via_rest(fastapi_test_app):
    """Test the /metrics/popular_tags endpoint."""
    response = fastapi_test_app.get("/metrics/popular_tags?project=demo_project")
    assert response.status_code == 200
    data = response.json()

    assert "popular_tags" in data
    assert "metadata" in data

    metadata = data["metadata"]
    assert "totalFeatureViews" in metadata
    assert "totalTags" in metadata
    assert "limit" in metadata

    assert metadata["totalFeatureViews"] >= 3
    assert metadata["totalTags"] >= 3  # environment, team, version

    popular_tags = data["popular_tags"]
    assert isinstance(popular_tags, list)
    assert len(popular_tags) > 0

    for tag_info in popular_tags:
        assert "tag_key" in tag_info
        assert "tag_value" in tag_info
        assert "feature_views" in tag_info
        assert "total_feature_views" in tag_info
        assert isinstance(tag_info["total_feature_views"], int)
        assert tag_info["total_feature_views"] > 0
        assert isinstance(tag_info["feature_views"], list)

        for fv in tag_info["feature_views"]:
            assert "name" in fv
            assert "project" in fv
            assert isinstance(fv["name"], str)
            assert isinstance(fv["project"], str)

    response = fastapi_test_app.get(
        "/metrics/popular_tags?project=demo_project&limit=2"
    )
    assert response.status_code == 200
    data = response.json()

    assert len(data["popular_tags"]) <= 2

    response = fastapi_test_app.get("/metrics/popular_tags")
    assert response.status_code == 200
    data = response.json()

    assert "popular_tags" in data
    assert "metadata" in data

    popular_tags = data["popular_tags"]
    assert isinstance(popular_tags, list)
    # May be empty if no feature views exist, but structure should be correct
    if len(popular_tags) > 0:
        for tag_info in popular_tags:
            assert "tag_key" in tag_info
            assert "tag_value" in tag_info
            assert "feature_views" in tag_info
            assert "total_feature_views" in tag_info


def test_all_endpoints_return_404_for_invalid_objects(fastapi_test_app):
    """Test that all REST API endpoints return 404 errors when objects are not found."""

    # Test entities endpoint
    response = fastapi_test_app.get(
        "/entities/non_existent_entity?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test feature views endpoint
    response = fastapi_test_app.get(
        "/feature_views/non_existent_fv?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test feature services endpoint
    response = fastapi_test_app.get(
        "/feature_services/non_existent_fs?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test data sources endpoint
    response = fastapi_test_app.get(
        "/data_sources/non_existent_ds?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test saved datasets endpoint
    response = fastapi_test_app.get(
        "/saved_datasets/non_existent_sd?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test features endpoint
    response = fastapi_test_app.get(
        "/features/non_existent_fv/non_existent_feature?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test projects endpoint
    response = fastapi_test_app.get("/projects/non_existent_project")
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"

    # Test permissions endpoint
    response = fastapi_test_app.get(
        "/permissions/non_existent_perm?project=demo_project"
    )
    assert response.status_code == 404
    data = response.json()
    assert data["status_code"] == 404
    assert data["error_type"] == "FeastObjectNotFoundException"
