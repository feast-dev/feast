import logging
import os
import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from feast import Entity, FeatureService, FeatureView, Field, FileSource, RequestSource
from feast.api.registry.rest.rest_registry_server import RestRegistryServer
from feast.feature_store import FeatureStore
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.on_demand_feature_view import on_demand_feature_view
from feast.project import Project
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset
from feast.types import Float64, Int64, String
from feast.value_type import ValueType

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.fixture
def search_test_app():
    """Test fixture that sets up a Feast environment with multiple resources for search testing"""
    # Create temp registry and data directory
    tmp_dir = tempfile.TemporaryDirectory()
    registry_path = os.path.join(tmp_dir.name, "registry.db")

    # Create dummy parquet files for different data sources
    user_data_path = os.path.join(tmp_dir.name, "user_data.parquet")
    product_data_path = os.path.join(tmp_dir.name, "product_data.parquet")
    transaction_data_path = os.path.join(tmp_dir.name, "transaction_data.parquet")

    # Create user data
    user_df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "age": [25, 30, 22],
            "income": [50000.0, 60000.0, 45000.0],
            "event_timestamp": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03"]
            ),
        }
    )
    user_df.to_parquet(user_data_path)

    # Create product data
    product_df = pd.DataFrame(
        {
            "product_id": [101, 102, 103],
            "price": [29.99, 15.99, 99.99],
            "category": ["electronics", "books", "electronics"],
            "event_timestamp": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03"]
            ),
        }
    )
    product_df.to_parquet(product_data_path)

    # Create transaction data
    transaction_df = pd.DataFrame(
        {
            "transaction_id": [1001, 1002, 1003],
            "amount": [100.0, 50.0, 200.0],
            "payment_method": ["credit", "debit", "credit"],
            "event_timestamp": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03"]
            ),
        }
    )
    transaction_df.to_parquet(transaction_data_path)

    # Setup repo config
    config = {
        "registry": registry_path,
        "project": "test_project",
        "provider": "local",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": ":memory:"},
    }

    # Create data sources
    user_source = FileSource(
        name="user_source",
        path=user_data_path,
        event_timestamp_column="event_timestamp",
    )

    product_source = FileSource(
        name="product_source",
        path=product_data_path,
        event_timestamp_column="event_timestamp",
    )

    transaction_source = FileSource(
        name="transaction_source",
        path=transaction_data_path,
        event_timestamp_column="event_timestamp",
    )

    # Create feature store
    store = FeatureStore(config=RepoConfig.model_validate(config))

    # Create entities
    user_entity = Entity(
        name="user",
        value_type=ValueType.INT64,
        description="User entity for customer data",
        tags={"team": "data", "environment": "test"},
    )

    product_entity = Entity(
        name="product",
        value_type=ValueType.INT64,
        description="Product entity for catalog data",
        tags={"team": "product", "environment": "test"},
    )

    transaction_entity = Entity(
        name="transaction",
        value_type=ValueType.INT64,
        description="Transaction entity for payment data",
        tags={"team": "finance", "environment": "test"},
    )

    # Create feature views
    user_features = FeatureView(
        name="user_features",
        entities=[user_entity],
        ttl=None,
        schema=[
            Field(name="age", dtype=Int64),
            Field(name="income", dtype=Float64),
        ],
        source=user_source,
        description="User demographic features",
        tags={"team": "data", "version": "v1"},
    )

    product_features = FeatureView(
        name="product_features",
        entities=[product_entity],
        ttl=None,
        schema=[
            Field(name="price", dtype=Float64),
            Field(name="category", dtype=String),
        ],
        source=product_source,
        description="Product catalog features",
        tags={"team": "product", "version": "v2"},
    )

    transaction_features = FeatureView(
        name="transaction_features",
        entities=[transaction_entity],
        ttl=None,
        schema=[
            Field(name="amount", dtype=Float64),
            Field(name="payment_method", dtype=String),
        ],
        source=transaction_source,
        description="Transaction payment features",
        tags={"team": "finance", "version": "v1"},
    )

    # Create feature services
    user_service = FeatureService(
        name="user_service",
        features=[user_features],
        description="Service for user-related features",
        tags={"team": "data", "type": "serving"},
    )

    product_service = FeatureService(
        name="product_service",
        features=[product_features],
        description="Service for product catalog features",
        tags={"team": "product", "type": "serving"},
    )

    # Create an on-demand feature view
    request_source = RequestSource(
        name="user_request_source",
        schema=[
            Field(name="user_id", dtype=Int64),
            Field(name="conversion_rate", dtype=Float64),
        ],
    )

    @on_demand_feature_view(
        sources=[user_features, request_source],
        schema=[
            Field(name="age_conversion_score", dtype=Float64),
        ],
        description="On-demand features combining user features with real-time data",
        tags={"team": "data", "type": "real_time", "environment": "test"},
    )
    def user_on_demand_features(inputs: dict):
        # Access individual feature columns directly from inputs
        age = inputs["age"]  # from user_features feature view
        conversion_rate = inputs["conversion_rate"]  # from request source

        # Create age-based conversion score
        age_conversion_score = age * conversion_rate

        return pd.DataFrame(
            {
                "age_conversion_score": age_conversion_score,
            }
        )

    # Create saved datasets
    user_dataset_storage = SavedDatasetFileStorage(path=user_data_path)
    user_dataset = SavedDataset(
        name="user_training_dataset",
        features=["user_features:age", "user_features:income"],
        join_keys=["user"],
        storage=user_dataset_storage,
        tags={"environment": "test", "purpose": "training", "team": "data"},
    )

    # Apply all objects
    store.apply(
        [
            user_entity,
            product_entity,
            transaction_entity,
            user_features,
            product_features,
            transaction_features,
            user_service,
            product_service,
            user_on_demand_features,
        ]
    )
    store._registry.apply_saved_dataset(user_dataset, "test_project")

    global global_store
    global_store = store

    # Build REST app
    rest_server = RestRegistryServer(store)
    client = TestClient(rest_server.app)

    yield client

    tmp_dir.cleanup()


@pytest.fixture
def multi_project_search_test_app():
    """Test fixture that sets up multiple projects with overlapping resource names for comprehensive multi-project search testing"""
    # Create temp registry and data directory
    tmp_dir = tempfile.TemporaryDirectory()
    registry_path = os.path.join(tmp_dir.name, "registry.db")

    # Create dummy parquet files for different projects with proper entity columns
    data_paths = {}
    entity_data = {
        "project_a": {
            "user_id": [1, 2, 3],
            "driver_id": [11, 12, 13],
            "trip_id": [21, 22, 23],
        },
        "project_b": {
            "user_id": [4, 5, 6],
            "restaurant_id": [14, 15, 16],
            "order_id": [24, 25, 26],
        },
        "project_c": {
            "customer_id": [7, 8, 9],
            "product_id": [17, 18, 19],
            "transaction_id": [27, 28, 29],
        },
    }

    for project in ["project_a", "project_b", "project_c"]:
        data_paths[project] = os.path.join(tmp_dir.name, f"{project}_data.parquet")

        # Create comprehensive data with all entity IDs and feature columns for this project
        base_data = {
            "event_timestamp": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03"]
            )
        }

        # Add entity columns for this project
        for entity_col, values in entity_data[project].items():
            base_data[entity_col] = values

        # Add feature columns that will be used by feature views
        feature_columns = {
            "user_features_value": [10.0, 20.0, 30.0],
            "feature_1_value": [11.0, 21.0, 31.0],
            "feature_2_value": [12.0, 22.0, 32.0],
            "driver_features_value": [13.0, 23.0, 33.0],
            "restaurant_features_value": [14.0, 24.0, 34.0],
            "customer_analytics_value": [15.0, 25.0, 35.0],
            "product_analytics_value": [16.0, 26.0, 36.0],
            "sales_features_value": [17.0, 27.0, 37.0],
        }

        for feature_col, values in feature_columns.items():
            base_data[feature_col] = values

        df = pd.DataFrame(base_data)
        df.to_parquet(data_paths[project])

    # Setup projects with overlapping resource names
    projects_data = {
        "project_a": {
            "description": "Ride sharing platform project",
            "domain": "transportation",
            "entities": [
                {"name": "user", "desc": "User entity for ride sharing"},
                {"name": "driver", "desc": "Driver entity for ride sharing"},
                {"name": "trip", "desc": "Trip entity for ride tracking"},
            ],
            "feature_views": [
                {
                    "name": "user_features",
                    "desc": "User demographic and rating features for rides",
                },
                {"name": "driver_features", "desc": "Driver performance and ratings"},
                {"name": "trip_features", "desc": "Trip duration and cost features"},
            ],
            "feature_services": [
                {
                    "name": "user_service",
                    "desc": "Service for user features in ride sharing",
                },
                {"name": "driver_service", "desc": "Service for driver matching"},
            ],
            "data_sources": [
                {"name": "user_data", "desc": "User data source for ride sharing"},
                {"name": "common_analytics", "desc": "Common analytics data source"},
            ],
        },
        "project_b": {
            "description": "Food delivery platform project",
            "domain": "food_delivery",
            "entities": [
                {
                    "name": "user",
                    "desc": "User entity for food delivery",
                },  # Same name as project_a
                {"name": "restaurant", "desc": "Restaurant entity for food delivery"},
                {"name": "order", "desc": "Order entity for food tracking"},
            ],
            "feature_views": [
                {
                    "name": "user_features",
                    "desc": "User preferences and order history for food",
                },  # Same name as project_a
                {
                    "name": "restaurant_features",
                    "desc": "Restaurant ratings and cuisine types",
                },
                {
                    "name": "order_features",
                    "desc": "Order value and delivery time features",
                },
            ],
            "feature_services": [
                {
                    "name": "user_service",
                    "desc": "Service for user features in food delivery",
                },  # Same name as project_a
                {
                    "name": "recommendation_service",
                    "desc": "Service for restaurant recommendations",
                },
            ],
            "data_sources": [
                {
                    "name": "restaurant_data",
                    "desc": "Restaurant data source for food delivery",
                },
                {
                    "name": "common_analytics",
                    "desc": "Common analytics data source",
                },  # Same name as project_a
            ],
        },
        "project_c": {
            "description": "E-commerce analytics project",
            "domain": "ecommerce",
            "entities": [
                {"name": "customer", "desc": "Customer entity for e-commerce"},
                {"name": "product", "desc": "Product entity for catalog"},
                {"name": "transaction", "desc": "Transaction entity for purchases"},
            ],
            "feature_views": [
                {"name": "customer_analytics", "desc": "Customer behavior analytics"},
                {"name": "product_analytics", "desc": "Product performance metrics"},
                {"name": "sales_features", "desc": "Sales and revenue features"},
            ],
            "feature_services": [
                {"name": "analytics_service", "desc": "Service for customer analytics"},
                {
                    "name": "product_service",
                    "desc": "Service for product recommendations",
                },
            ],
            "data_sources": [
                {"name": "sales_data", "desc": "Sales transaction data"},
                {"name": "inventory_data", "desc": "Product inventory data"},
            ],
        },
    }

    # Create a single registry to hold all projects
    base_config = {
        "registry": registry_path,
        "provider": "local",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": ":memory:"},
    }

    # Create a master FeatureStore instance for managing the shared registry
    master_config = {**base_config, "project": "project_a"}  # Use project_a as base
    master_store = FeatureStore(config=RepoConfig.model_validate(master_config))

    # First, create the Project objects in the registry

    for project_name, project_data in projects_data.items():
        project_obj = Project(
            name=project_name,
            description=project_data["description"],
            tags={"domain": project_data["domain"]},
        )
        master_store._registry.apply_project(project_obj)

    # Create resources for each project and apply them to the shared registry
    for project_name, project_data in projects_data.items():
        # Create data sources for this project
        data_sources = []
        for ds in project_data["data_sources"]:
            # Make data source names unique across projects to avoid conflicts
            unique_name = (
                f"{project_name}_{ds['name']}"
                if ds["name"] == "common_analytics"
                else ds["name"]
            )

            source = FileSource(
                name=unique_name,
                path=data_paths[project_name],
                event_timestamp_column="event_timestamp",
            )
            # Ensure the data source has the correct project set
            if hasattr(source, "project"):
                source.project = project_name
            data_sources.append(source)

        # Create entities for this project with proper join keys
        entities = []
        entity_mapping = {
            "project_a": {"user": "user_id", "driver": "driver_id", "trip": "trip_id"},
            "project_b": {
                "user": "user_id",
                "restaurant": "restaurant_id",
                "order": "order_id",
            },
            "project_c": {
                "customer": "customer_id",
                "product": "product_id",
                "transaction": "transaction_id",
            },
        }

        for ent in project_data["entities"]:
            join_key = entity_mapping[project_name][ent["name"]]
            entity = Entity(
                name=ent["name"],
                join_keys=[join_key],
                value_type=ValueType.INT64,  # Add required value_type
                description=ent["desc"],
                tags={
                    "project": project_name,
                    "domain": project_data["domain"],
                    "environment": "test",
                },
            )
            # Ensure the entity has the correct project set
            entity.project = project_name
            entities.append(entity)

        # Create feature views for this project with proper entity relationships
        feature_views = []

        # Map feature view names to their corresponding feature columns
        feature_column_mapping = {
            "user_features": "user_features_value",
            "driver_features": "driver_features_value",
            "trip_features": "feature_1_value",
            "restaurant_features": "restaurant_features_value",
            "order_features": "feature_2_value",
            "customer_analytics": "customer_analytics_value",
            "product_analytics": "product_analytics_value",
            "sales_features": "sales_features_value",
        }

        for i, fv in enumerate(project_data["feature_views"]):
            # Alternate between data sources and entities
            source = data_sources[i % len(data_sources)]
            entity = entities[i % len(entities)]  # Use different entities

            # Get the correct feature column name for this feature view
            feature_column = feature_column_mapping.get(
                fv["name"], f"feature_{i}_value"
            )

            # Get the entity's join key for the schema
            entity_join_key = entity.join_key

            feature_view = FeatureView(
                name=fv["name"],
                entities=[entity],
                ttl=None,
                schema=[
                    # Include entity column in schema
                    Field(name=entity_join_key, dtype=Int64),
                    # Include feature column in schema
                    Field(name=feature_column, dtype=Float64),
                ],
                source=source,
                description=fv["desc"],
                tags={
                    "project": project_name,
                    "domain": project_data["domain"],
                    "team": f"team_{project_name}",
                    "version": f"v{i + 1}",
                },
            )
            # Ensure the feature view has the correct project set
            feature_view.project = project_name
            feature_views.append(feature_view)

        # Create feature services for this project
        feature_services = []
        for i, fs in enumerate(project_data["feature_services"]):
            # Use different feature views for each service
            fv_subset = (
                feature_views[i : i + 2]
                if i + 1 < len(feature_views)
                else [feature_views[i]]
            )

            service = FeatureService(
                name=fs["name"],
                features=fv_subset,
                description=fs["desc"],
                tags={
                    "project": project_name,
                    "domain": project_data["domain"],
                    "service_type": "real_time",
                },
            )
            # Ensure the feature service has the correct project set
            service.project = project_name
            feature_services.append(service)

        # Apply all objects for this project directly to the registry
        for entity in entities:
            master_store._registry.apply_entity(entity, project_name)

        for data_source in data_sources:
            master_store._registry.apply_data_source(data_source, project_name)

        for feature_view in feature_views:
            master_store._registry.apply_feature_view(feature_view, project_name)

        for feature_service in feature_services:
            master_store._registry.apply_feature_service(feature_service, project_name)

    # Ensure registry is committed
    master_store._registry.commit()

    # Build REST app using the master store's registry (contains all projects)
    rest_server = RestRegistryServer(master_store)
    client = TestClient(rest_server.app)

    yield client

    tmp_dir.cleanup()


@pytest.fixture
def shared_search_responses(search_test_app):
    """Pre-computed responses for common search scenarios to reduce API calls"""
    return {
        "user_query": search_test_app.get("/search?query=user").json(),
        "empty_query": search_test_app.get("/search?query=").json(),
        "nonexistent_query": search_test_app.get("/search?query=xyz_12345").json(),
        "paginated_basic": search_test_app.get("/search?query=&page=1&limit=5").json(),
        "paginated_page2": search_test_app.get("/search?query=&page=2&limit=3").json(),
        "sorted_by_name": search_test_app.get(
            "/search?query=&sort_by=name&sort_order=asc"
        ).json(),
        "sorted_by_match_score": search_test_app.get(
            "/search?query=user&sort_by=match_score&sort_order=desc"
        ).json(),
        "with_tags": search_test_app.get("/search?query=&tags=team:data").json(),
        "feature_name_query": search_test_app.get("/search?query=age").json(),
    }


class TestSearchAPI:
    """Test class for the comprehensive search API"""

    def test_search_user_query_comprehensive(self, shared_search_responses):
        """Comprehensive test for user query validation - combines multiple test scenarios"""
        data = shared_search_responses["user_query"]

        # Test response structure (replaces test_search_all_resources_with_query)
        assert "results" in data
        assert "pagination" in data
        assert "query" in data
        assert "projects_searched" in data
        assert "errors" in data
        assert data["query"] == "user"

        # Test pagination structure
        pagination = data["pagination"]
        assert pagination["totalCount"] > 0
        assert pagination["totalPages"] > 0
        assert pagination["page"] == 1
        assert pagination["limit"] == 50

        # Test results content
        results = data["results"]
        assert len(results) > 0
        result = results[0]
        required_result_fields = [
            "type",
            "name",
            "description",
            "project",
            "match_score",
        ]
        for field in required_result_fields:
            assert field in result

        # Log for debugging
        type_counts = {}
        for r in results:
            result_type = r.get("type", "unknown")
            type_counts[result_type] = type_counts.get(result_type, 0) + 1

        logger.debug(f"Found {len(results)} results:")
        for r in results:
            logger.debug(
                f"  - {r['type']}: {r['name']} (score: {r.get('match_score', 'N/A')})"
            )

        # Test that we found expected resources
        resource_names = [r["name"] for r in results]
        assert "user" in resource_names  # user entity

        # Test feature views
        feature_view_names = [r["name"] for r in results if r["type"] == "featureView"]
        if feature_view_names:
            assert "user_features" in feature_view_names
        else:
            logging.warning(
                "No feature views found in search results - this may indicate a search API issue"
            )

        # Test cross-project functionality (replaces test_search_cross_project_when_no_project_specified)
        assert len(data["projects_searched"]) >= 1
        assert "test_project" in data["projects_searched"]

    def test_search_with_project_filter(self, search_test_app):
        """Test searching within a specific project"""
        response = search_test_app.get("/search?query=user&projects=test_project")
        assert response.status_code == 200

        data = response.json()
        assert data["projects_searched"] == ["test_project"]

        results = data["results"]
        # All results should be from test_project
        for result in results:
            if "project" in result:
                assert result["project"] == "test_project"

    def test_search_by_description(self, search_test_app):
        """Test searching by description content"""
        response = search_test_app.get("/search?query=demographic")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Debug: Show what we found
        logger.debug(f"Search for 'demographic' returned {len(results)} results:")
        for r in results:
            logger.debug(
                f"  - {r['type']}: {r['name']} - '{r.get('description', '')}' (score: {r.get('match_score', 'N/A')})"
            )

        # Should find user_features which has "demographic" in description
        feature_view_names = [r["name"] for r in results if r["type"] == "featureView"]
        if len(feature_view_names) > 0:
            assert "user_features" in feature_view_names
        else:
            # If no feature views found, check if any resources have "demographic" in description
            demographic_resources = [
                r for r in results if "demographic" in r.get("description", "").lower()
            ]
            if len(demographic_resources) == 0:
                logger.warning(
                    "No resources found with 'demographic' in description - search may not be working properly"
                )

    def test_search_by_tags(self, shared_search_responses):
        """Test searching by tag content"""
        # Get tags filtered results
        tags_data = shared_search_responses["with_tags"]
        logger.debug(f"Tags data: {tags_data}")
        results = tags_data["results"]
        assert len(results) > 0

        # Should find user-related resources that also have "team": "data" tag
        expected_resources = {"user", "user_features", "user_service"}
        found_resources = {r["name"] for r in results}

        # Check intersection rather than strict subset (more flexible)
        found_expected = expected_resources.intersection(found_resources)
        assert len(found_expected) > 0, (
            f"Expected to find some of {expected_resources} but found none in {found_resources}"
        )

    def test_search_matched_tags_exact_match(self, search_test_app):
        """Test that matched_tags field is present when a tag matches exactly"""
        # Search for "data" which should match tag key "team" with value "data"
        response = search_test_app.get("/search?query=data")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Find results that matched via tags (match_score = 60)
        tag_matched_results = [
            r for r in results if r.get("match_score") == 60 and "matched_tags" in r
        ]

        assert len(tag_matched_results) > 0, (
            "Expected to find at least one result with matched_tags from tag matching"
        )

        # Verify matched_tags is present and has a valid dictionary value
        for result in tag_matched_results:
            matched_tags = result.get("matched_tags")
            assert matched_tags is not None, (
                f"matched_tags should not be None for result {result['name']}"
            )
            assert isinstance(matched_tags, dict), (
                f"matched_tags should be a dictionary, got {type(matched_tags)}"
            )
            # matched_tags should be a non-empty dict for tag-matched results
            assert len(matched_tags) > 0, (
                "matched_tags should not be empty for tag matches"
            )

        logger.debug(
            f"Found {len(tag_matched_results)} results with matched_tags: {[r['name'] + ' -> ' + str(r.get('matched_tags', 'N/A')) for r in tag_matched_results]}"
        )

    def test_search_matched_tags_fuzzy_match(self, search_test_app):
        """Test that matched_tags field is present when a tag matches via fuzzy matching"""
        # Search for "te" which should fuzzy match tag key "team"
        # "te" vs "team": overlap={'t','e'}/union={'t','e','a','m'} = 2/4 = 50% (below threshold)
        # Try "tea" which should fuzzy match "team" better
        # "tea" vs "team": overlap={'t','e','a'}/union={'t','e','a','m'} = 3/4 = 75% (above threshold)
        response = search_test_app.get("/search?query=tea")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Find results that matched via fuzzy tag matching (match_score < 60 but >= 40)
        fuzzy_tag_matched_results = [
            r
            for r in results
            if r.get("match_score", 0) >= 40
            and r.get("match_score", 0) < 60
            and "matched_tags" in r
        ]

        # If we don't find fuzzy matches, try a different query that's more likely to match
        if len(fuzzy_tag_matched_results) == 0:
            # Try "dat" which should fuzzy match tag value "data"
            # "dat" vs "data": overlap={'d','a','t'}/union={'d','a','t','a'} = 3/4 = 75% (above threshold)
            response = search_test_app.get("/search?query=dat")
            assert response.status_code == 200
            data = response.json()
            results = data["results"]
            fuzzy_tag_matched_results = [
                r
                for r in results
                if r.get("match_score", 0) >= 40
                and r.get("match_score", 0) < 60
                and "matched_tags" in r
            ]

        if len(fuzzy_tag_matched_results) > 0:
            # Verify matched_tags is present for fuzzy matches
            for result in fuzzy_tag_matched_results:
                matched_tags = result.get("matched_tags")
                assert matched_tags is not None, (
                    f"matched_tags should not be None for fuzzy-matched result {result['name']}"
                )
                assert isinstance(matched_tags, dict), (
                    f"matched_tags should be a dictionary, got {type(matched_tags)}"
                )
                assert len(matched_tags) > 0, (
                    "matched_tags should not be empty for fuzzy tag matches"
                )
                # Verify the match_score is in the fuzzy range
                assert 40 <= result.get("match_score", 0) < 60, (
                    f"Fuzzy tag match should have score in [40, 60), got {result.get('match_score')}"
                )

            logger.debug(
                f"Found {len(fuzzy_tag_matched_results)} results with fuzzy matched_tags: {[r['name'] + ' -> ' + str(r.get('matched_tags', 'N/A')) + ' (score: ' + str(r.get('match_score', 'N/A')) + ')' for r in fuzzy_tag_matched_results]}"
            )

    def test_search_sorting_functionality(self, shared_search_responses):
        """Test search results sorting using pre-computed responses"""
        # Test match_score descending sort
        match_score_data = shared_search_responses["sorted_by_match_score"]
        results = match_score_data["results"]
        if len(results) > 1:
            for i in range(len(results) - 1):
                current_score = results[i].get("match_score", 0)
                next_score = results[i + 1].get("match_score", 0)
                assert current_score >= next_score, (
                    "Results not sorted descending by match_score"
                )

        # Test name ascending sort
        name_data = shared_search_responses["sorted_by_name"]
        results = name_data["results"]
        if len(results) > 1:
            for i in range(len(results) - 1):
                current_name = results[i].get("name", "")
                next_name = results[i + 1].get("name", "")
                assert current_name <= next_name, "Results not sorted ascending by name"

    def test_search_query_functionality(self, shared_search_responses):
        """Test basic search functionality with different query types using shared responses"""
        # Test empty query returns all resources
        empty_data = shared_search_responses["empty_query"]
        assert len(empty_data["results"]) > 0
        assert empty_data["query"] == ""

        results = empty_data["results"]

        # Get all resource types returned
        returned_types = set(result["type"] for result in results)

        # Should include all expected resource types (including new 'feature' type)
        expected_types = {
            "entity",
            "featureView",
            "feature",
            "featureService",
            "dataSource",
            "savedDataset",
        }

        # All expected types should be present (or at least no filtering happening)
        # Note: Some types might not exist in test data, but if they do exist, they should all be returned
        available_types_in_data = expected_types.intersection(returned_types)
        assert len(available_types_in_data) >= 4, (
            f"Expected multiple resource types in results, but only got {returned_types}. "
            "All available resource types should be searched."
        )

        # Verify feature result structure
        for result in results:
            # Check required fields
            assert "type" in result
            assert "name" in result
            assert "description" in result
            assert "project" in result

        # Get all feature results
        feature_results = [result for result in results if result["type"] == "feature"]

        # Should have individual features in search results
        assert len(feature_results) > 0, (
            "Expected individual features to appear in search results, but found none"
        )

        for feature_result in feature_results:
            assert "featureView" in feature_result
            assert feature_result["featureView"] in [
                "user_features",
                "product_features",
                "transaction_features",
                "user_on_demand_features",
            ]

        # Verify we have features that likely come from different feature views
        feature_names = {f["name"] for f in feature_results}

        # Based on test fixture features: age, income (from user_features), price, category (from product_features),
        # amount, payment_method (from transaction_features)
        expected_features = {
            "age",
            "income",
            "price",
            "category",
            "amount",
            "payment_method",
        }
        found_features = expected_features.intersection(feature_names)

        assert len(found_features) >= 3, (
            f"Expected features from multiple feature views, but only found features: {feature_names}. "
            f"Expected to find at least 3 of: {expected_features}"
        )

        # Get all feature view results to understand the source feature views
        feature_view_results = [
            result for result in results if result["type"] == "featureView"
        ]
        feature_view_names = {fv["name"] for fv in feature_view_results}

        # Based on test fixture: user_features, product_features, transaction_features
        expected_feature_views = {
            "user_features",
            "product_features",
            "transaction_features",
        }

        # Should have feature views from test fixture
        found_feature_views = expected_feature_views.intersection(feature_view_names)
        assert len(found_feature_views) >= 2, (
            f"Expected features from multiple feature views, but only found feature views: {feature_view_names}. "
            f"Expected to find some of: {expected_feature_views}"
        )

        # Test nonexistent query
        nonexistent_data = shared_search_responses["nonexistent_query"]
        logger.debug(f"Nonexistent data: {nonexistent_data}")
        assert len(nonexistent_data["results"]) == 0

        # Search for a specific feature name 'age'
        age_feature_response = shared_search_responses["feature_name_query"]

        results = age_feature_response["results"]

        # Should find feature named "age"
        age_features = [
            result
            for result in results
            if result["type"] == "feature" and "age" in result["name"].lower()
        ]

        assert len(age_features) > 0, (
            "Expected to find feature named 'age' in search results"
        )

    def test_search_fuzzy_matching(self, search_test_app):
        """Test fuzzy matching functionality with assumed threshold of 0.6"""
        # Assumption: fuzzy matching threshold is 0.6 (60% similarity)
        # "usr" should match "user" as it's a partial match with reasonable similarity
        response = search_test_app.get("/search?query=usr")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Should find user-related resources due to fuzzy matching
        user_matches = [r for r in results if "user" in r["name"].lower()]

        if len(user_matches) > 0:
            # If fuzzy matching works, verify match scores are reasonable but lower than exact matches
            for match in user_matches:
                match_score = match.get("match_score", 0)
                # Fuzzy matches should have lower scores than exact matches (< 80)
                # but still above minimum threshold (>= 40 for reasonable partial matches)
                assert 40 <= match_score < 80, (
                    f"Fuzzy match score {match_score} outside expected range [40, 80) for {match['name']}"
                )

        # Test with closer match - "use" should definitely match "user" if fuzzy matching enabled
        response = search_test_app.get("/search?query=use")
        assert response.status_code == 200

        data = response.json()
        close_matches = [r for r in data["results"] if "user" in r["name"].lower()]

        # "use" is closer to "user" than "usr", so should have better chance of matching
        # If fuzzy matching is implemented, this should find matches
        logger.debug(f"'use' query found {len(close_matches)} user-related matches")
        for match in close_matches:
            logger.debug(
                f"  - {match['name']}: score {match.get('match_score', 'N/A')}"
            )

    def test_search_api_special_characters(self, search_test_app):
        """Test search API with special characters in query and verify expected results"""
        # Define expected matches for each special character query
        # NOTE: Queries are designed to achieve 75%+ similarity with fuzzy matching algorithm
        special_query_expectations = {
            "users": {
                "should_find": [
                    "user"
                ],  # "users" vs "user": overlap={'u','s','e','r'}/union={'u','s','e','r','s'} = 4/5 = 80%
                "description": "Plural form should find user entity",
            },
            "user_feature": {
                "should_find": [
                    "user_features",
                ],  # "user_feature" vs "user_features": overlap={'u','s','e','r','_','f','a','t','u','r'}/union={'u','s','e','r','_','f','a','t','u','r','e','s'} = 10/12 = 83%
                "description": "Singular form should find feature views",
            },
            "product": {
                "should_find": [
                    "product",
                    "product_features",
                    "product_source",
                ],  # "product" vs "product": 100% match ✅
                "description": "Exact match should find product resources",
            },
            "sources": {
                "should_find": [
                    "user_source",
                    "product_source",
                    "transaction_source",
                ],  # "sources" vs "user_source": overlap={'s','o','u','r','c','e'}/union={'s','o','u','r','c','e','_','u'} = 6/8 = 75%
                "description": "Plural form should find data sources",
            },
        }

        for query, expectation in special_query_expectations.items():
            response = search_test_app.get(f"/search?query={query}")
            assert response.status_code == 200

            data = response.json()
            assert "results" in data
            assert isinstance(data["results"], list)
            assert data["pagination"]["totalCount"] > 0

            results = data["results"]
            found_names = {r["name"] for r in results}
            expected_names = set(expectation["should_find"])

            logger.debug(
                f"Query '{query}' found {len(results)} results: {list(found_names)}"
            )
            logger.debug(
                f"       Expected to find: {list(expected_names)} - {expectation['description']}"
            )

            # Check if we found at least some of the expected resources
            # Use intersection since search might be fuzzy and return additional results
            found_expected = expected_names.intersection(found_names)

            if len(found_expected) > 0:
                # If we found some expected resources, verify they have reasonable match scores
                for result in results:
                    if result["name"] in expected_names:
                        match_score = result.get("match_score", 0)
                        assert match_score > 0, (
                            f"Expected positive match score for '{result['name']}' but got {match_score}"
                        )

            # Verify query echo-back works with special characters
            assert data["query"] == query, (
                f"Query echo-back failed for special characters: expected '{query}' but got '{data['query']}'"
            )

    def test_search_specific_multiple_projects(self, search_test_app):
        response = search_test_app.get(
            "/search?query=user&projects=test_project&projects=another_project"
        )
        assert response.status_code == 200

        data = response.json()
        results = data.get("results", [])
        project_counts = {}
        for result in results:
            project = result.get("project", "unknown")
            project_counts[project] = project_counts.get(project, 0) + 1

        assert "projects_searched" in data
        # Should search only existing projects, non-existing ones are ignored
        expected_projects = ["test_project"]  # only existing project
        assert data["projects_searched"] == expected_projects
        logger.debug(f"Errors: {data['errors']}")
        assert "Following projects do not exist: another_project" in data["errors"]
        assert data["errors"] == ["Following projects do not exist: another_project"]

        # Results should include project information
        for result in data["results"]:
            if "project" in result:
                assert result["project"] in expected_projects

    def test_search_empty_projects_parameter_searches_all(self, search_test_app):
        """Test that empty projects parameter still searches all projects"""
        response = search_test_app.get("/search?query=user&projects=")
        assert response.status_code == 200

        data = response.json()
        # Should search all available projects (at least test_project)
        assert len(data["projects_searched"]) >= 1
        assert "test_project" in data["projects_searched"]

    def test_search_nonexistent_projects(self, search_test_app):
        """Test searching in projects that don't exist"""
        response = search_test_app.get(
            "/search?query=user&projects=nonexistent1&projects=nonexistent2"
        )
        assert response.status_code == 200

        data = response.json()
        assert data["projects_searched"] == []  # no existing projects to search
        # Should return empty results since projects don't exist
        assert data["results"] == []
        assert not data["pagination"].get("totalCount", False)
        assert len(data["errors"]) == 1
        for proj in ["nonexistent1", "nonexistent2"]:
            assert proj in data["errors"][0]

    def test_search_many_projects_performance(self, search_test_app):
        """Test search performance with many projects"""
        # Create a list of many projects (mix of existing and non-existing)
        fake_projects = [f"fake_project_{i}" for i in range(20)]
        many_projects = ["test_project"] + fake_projects
        projects_param = "&".join([f"projects={p}" for p in many_projects])

        response = search_test_app.get(f"/search?query=user&{projects_param}")
        assert response.status_code == 200

        data = response.json()
        assert len(data["projects_searched"]) == 1  # only 1 real project exists
        assert "test_project" in data["projects_searched"]
        assert len(data["errors"]) == 1

        for proj in fake_projects:
            assert proj in data["errors"][0]

        # Should still return results from the one existing project
        if data["results"]:
            for result in data["results"]:
                if "project" in result:
                    assert result["project"] == "test_project"

    def test_search_duplicate_projects_deduplication(self, search_test_app):
        """Test that duplicate projects in list are handled properly"""
        response = search_test_app.get(
            "/search?query=user&projects=test_project&projects=test_project&projects=test_project"
        )
        assert response.status_code == 200

        data = response.json()
        # API should handle duplicates gracefully (may or may not deduplicate)
        # At minimum, should not crash and should search test_project
        assert len(data["projects_searched"]) == 1
        assert "test_project" == data["projects_searched"][0]

    def test_search_on_demand_feature_view(self, search_test_app):
        """Test searching for on-demand feature views"""
        # Search by name
        global global_store
        global_store._registry.refresh()
        response = search_test_app.get("/search?query=user_on_demand_features")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Should find the on-demand feature view
        on_demand_fv_results = [r for r in results if r["type"] == "featureView"]
        assert len(on_demand_fv_results) > 0

        on_demand_fv = on_demand_fv_results[0]
        logger.debug(f"On-demand feature view: {on_demand_fv_results}")
        assert on_demand_fv["name"] == "user_on_demand_features"
        assert (
            "On-demand features combining user features with real-time data"
            in on_demand_fv["description"]
        )
        assert on_demand_fv["project"] == "test_project"
        assert "match_score" in on_demand_fv
        assert on_demand_fv["match_score"] > 0

        # Search by description content
        response = search_test_app.get("/search?query=real-time")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Should find the on-demand feature view by description
        on_demand_description_results = [
            r
            for r in results
            if "real-time" in r.get("description", "").lower()
            or "real_time" in r.get("description", "").lower()
        ]
        assert len(on_demand_description_results) > 0

        # Check that our on-demand feature view is in the results
        on_demand_names = [r["name"] for r in on_demand_description_results]
        assert "user_on_demand_features" in on_demand_names

        # Search by tags
        response = search_test_app.get("/search?query=&tags=type:real_time")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Should find the on-demand feature view by tag
        tagged_results = [r for r in results if r["name"] == "user_on_demand_features"]
        assert len(tagged_results) > 0

        tagged_result = tagged_results[0]
        assert tagged_result["type"] == "featureView"
        assert tagged_result["name"] == "user_on_demand_features"

    def test_search_on_demand_features_individual(self, search_test_app):
        """Test searching for individual features from on-demand feature views"""
        # Search for individual features from the on-demand feature view
        response = search_test_app.get("/search?query=age_conversion_score")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Should find the individual feature from the on-demand feature view
        feature_results = [
            r
            for r in results
            if r["type"] == "feature" and r["name"] == "age_conversion_score"
        ]
        assert len(feature_results) > 0

        feature_result = feature_results[0]
        assert feature_result["name"] == "age_conversion_score"
        assert feature_result["type"] == "feature"
        assert feature_result["project"] == "test_project"
        assert "match_score" in feature_result
        assert feature_result["match_score"] == 100  # Exact match should have score 100

        # Verify that features from different feature view types can be found together
        response = search_test_app.get("/search?query=&sort_by=name&sort_order=asc")
        assert response.status_code == 200

        data = response.json()
        all_features = [r for r in data["results"] if r["type"] == "feature"]

        # Should have features from both regular feature views and on-demand feature views
        regular_features = []
        on_demand_features = []

        for feature in all_features:
            if feature["name"] in [
                "age",
                "income",
                "price",
                "category",
                "amount",
                "payment_method",
            ]:
                regular_features.append(feature)
            elif feature["name"] in ["age_conversion_score"]:
                on_demand_features.append(feature)

        assert len(regular_features) > 0, (
            "Should have features from regular feature views"
        )
        assert len(on_demand_features) > 0, (
            "Should have features from on-demand feature views"
        )

        logger.debug(
            f"Found {len(regular_features)} regular features and {len(on_demand_features)} on-demand features"
        )

    def test_search_missing_required_query_parameter(self, search_test_app):
        """Test search API fails when required query parameter is missing"""
        response = search_test_app.get("/search")
        assert response.status_code == 422  # Unprocessable Entity

        error_data = response.json()
        assert "detail" in error_data
        logger.debug(f"Error data: {error_data}")
        # FastAPI should return validation error for missing required field
        assert "query" in str(error_data["detail"]).lower()

    @pytest.mark.parametrize(
        "test_cases",
        [
            [
                ("sort_by", "invalid_sort_field", "sort_order", "desc", 400),
                ("sort_by", "name", "sort_order", "invalid_order", 400),
                ("sort_by", "", "sort_order", "asc", 200),
                ("sort_by", "match_score", "sort_order", "", 200),
                ("sort_by", "123", "sort_order", "xyz", 400),
                (
                    "allow_cache",
                    "invalid_bool",
                    None,
                    None,
                    422,
                ),  # FastAPI may handle gracefully
                (
                    "allow_cache",
                    "yes",
                    None,
                    None,
                    200,
                ),  # FastAPI converts to boolean
                (
                    "allow_cache",
                    "1",
                    None,
                    None,
                    200,
                ),  # FastAPI converts to boolean
            ],
        ],
    )
    def test_search_with_invalid_parameters(self, search_test_app, test_cases):
        """Test search API with various invalid parameter combinations"""
        logger.debug(f"Test cases: {test_cases}")
        for param1, value1, param2, value2, expected_code in test_cases:
            # Build query string
            query_parts = ["query=user"]
            query_parts.append(f"{param1}={value1}")
            if param2 is not None and value2 is not None:
                query_parts.append(f"{param2}={value2}")

            url = "/search?" + "&".join(query_parts)
            response = search_test_app.get(url)

            assert response.status_code == expected_code, (
                f"Expected {expected_code} but got {response.status_code} for {param1}='{value1}'"
                + (f", {param2}='{value2}'" if param2 else "")
            )

            if response.status_code == 200:
                # If successful, verify response format
                data = response.json()
                assert "results" in data
                assert isinstance(data["results"], list)
            elif response.status_code in [400, 422]:
                # If validation error, verify it's a proper FastAPI error
                error_data = response.json()
                assert "detail" in error_data

    def test_search_with_extremely_long_query(self, search_test_app):
        """Test search API with extremely long query string"""
        # Create a very long query (10KB)
        long_query = "a" * 10000

        response = search_test_app.get(f"/search?query={long_query}")
        assert response.status_code == 200  # Should handle large queries gracefully

        data = response.json()
        assert "results" in data
        assert data["query"] == long_query

    def test_search_with_malformed_and_edge_case_parameters(self, search_test_app):
        """Test search API with malformed parameters and edge case values"""
        # Test malformed tags
        malformed_tags = [
            "invalid_tag_format",
            "key1:value1:extra",
            "=value_without_key",
            "key_without_value=",
            "::",
            "key1=value1&key2",
            "key with spaces:value",
        ]

        for malformed_tag in malformed_tags:
            response = search_test_app.get(f"/search?query=test&tags={malformed_tag}")
            assert response.status_code == 200
            data = response.json()
            assert "results" in data

        # Test empty and null-like query values
        empty_scenarios = [
            ("", "empty string"),
            ("   ", "whitespace only"),
            ("null", "string 'null'"),
            ("undefined", "string 'undefined'"),
            ("None", "string 'None'"),
        ]

        for query_value, description in empty_scenarios:
            response = search_test_app.get(f"/search?query={query_value}")
            assert response.status_code == 200, f"Failed for {description}"
            data = response.json()
            assert "results" in data
            assert data["query"] == query_value

    def test_search_all_resource_types_individually(self, search_test_app):
        """Test that all resource types can be searched individually and return only that type"""

        pytest.skip("Skipping resource types filtering tests")

        # Expected counts based on test fixture data
        expected_counts = {
            "entities": 3,  # user, product, transaction
            "feature_views": 3,  # user_features, product_features, transaction_features
            "feature_services": 2,  # user_service, product_service
            "data_sources": 3,  # user_source, product_source, transaction_source
            "saved_datasets": 1,  # user_training_dataset
            "permissions": 0,  # No permissions in test data
            "projects": 1,  # test_project
        }

        for resource_type in expected_counts.keys():
            response = search_test_app.get(
                f"/search?query=&resource_types={resource_type}"
            )
            assert response.status_code == 200

            data = response.json()
            assert "results" in data
            assert isinstance(data["results"], list)

            results = data["results"]
            expected_count = expected_counts[resource_type]

            # Map plural resource_type to singular type names used in results
            type_mapping = {
                "entities": "entity",
                "feature_views": "featureView",
                "feature_services": "featureService",
                "data_sources": "dataSource",
                "saved_datasets": "savedDataset",
                "permissions": "permission",
                "projects": "project",
            }
            expected_type = type_mapping[resource_type]

            # Assert all results are of the requested type
            for result in results:
                assert result.get("type") == expected_type, (
                    f"Expected type '{expected_type}' but got '{result.get('type')}' for resource_type '{resource_type}'"
                )

            # Filter out Feast internal resources (like __dummy entity) for count validation
            if resource_type == "entities":
                # Feast automatically creates __dummy entity - filter it out for test validation
                filtered_results = [
                    r for r in results if not r.get("name", "").startswith("__")
                ]
                actual_count = len(filtered_results)
                logger.debug(
                    f"entities returned {len(results)} total results, {actual_count} non-internal (expected {expected_count})"
                )
                logger.debug(
                    f"       Internal entities filtered: {[r['name'] for r in results if r.get('name', '').startswith('__')]}"
                )
            else:
                filtered_results = results
                actual_count = len(filtered_results)
                logger.debug(
                    f"{resource_type} returned {actual_count} results (expected {expected_count})"
                )

            # Assert expected count (allow some flexibility for permissions/projects that might vary)
            if resource_type in ["permissions", "projects"]:
                assert actual_count >= 0, (
                    f"Resource type '{resource_type}' should return non-negative count"
                )
            else:
                assert actual_count == expected_count, (
                    f"Expected {expected_count} results for '{resource_type}' but got {actual_count} (after filtering internal resources)"
                )

    def test_search_specific_resource_types(self, search_test_app):
        """Test filtering by specific resource types"""

        pytest.skip("Skipping resource types filtering tests")
        # Search only entities
        response = search_test_app.get("/search?query=user&resource_types=entities")
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # All results should be entities
        for result in results:
            assert result["type"] == "entity"

        # Should find the user entity
        entity_names = [r["name"] for r in results]
        assert "user" in entity_names

    def test_search_multiple_resource_types(self, search_test_app):
        """Test filtering by multiple resource types"""

        pytest.skip("Skipping resource types filtering tests")

        response = search_test_app.get(
            "/search?query=product&resource_types=entities&resource_types=feature_views"
        )
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        # Results should only be entities or feature_views
        result_types = [r["type"] for r in results]
        for result_type in result_types:
            assert result_type in ["entity", "featureView"]

    def test_search_with_mixed_valid_invalid_resource_types(self, search_test_app):
        """Test search API with mix of valid and invalid resource types"""

        pytest.skip("Skipping resource types filtering tests")

        response = search_test_app.get(
            "/search?query=user&resource_types=entities&resource_types=invalid_type&resource_types=feature_views&resource_types=another_invalid"
        )
        assert response.status_code == 200

        data = response.json()
        # Should process valid types and ignore invalid ones
        assert "entities" in data["resource_types"]
        assert "feature_views" in data["resource_types"]
        assert "invalid_type" not in data["resource_types"]
        assert "another_invalid" not in data["resource_types"]

        # Results should only come from valid resource types
        if data["results"]:
            valid_types = {
                "entity",
                "featureView",
                "featureService",
                "dataSource",
                "savedDataset",
                "permission",
                "project",
            }
            for result in data["results"]:
                assert result.get("type") in valid_types or result.get("type") == ""

        # Test scenarios that should return 400 due to stricter validation
        scenarios_400 = [
            "/search?query=&sort_by=invalid",
        ]

        for scenario in scenarios_400:
            response = search_test_app.get(scenario)
            assert response.status_code == 400

    def test_search_with_invalid_resource_types(self, search_test_app):
        """Test search API with invalid resource types"""

        pytest.skip("Skipping resource types filtering tests")

        invalid_resource_types = [
            "invalid_type",
            "nonexistent_resource",
            "malformed_type",
            "",  # empty string
            "123",  # numeric
            "feature_views_typo",
        ]

        for invalid_type in invalid_resource_types:
            response = search_test_app.get(
                f"/search?query=test&resource_types={invalid_type}"
            )
            assert response.status_code == 200  # Should handle gracefully

            data = response.json()
            # Should return empty results for invalid types
            assert isinstance(data["results"], list)
            assert data["totalCount"] >= 0

    def test_search_with_multiple_invalid_resource_types(self, search_test_app):
        """Test search API with multiple invalid resource types"""

        pytest.skip("Skipping resource types filtering tests")

        response = search_test_app.get(
            "/search?query=test&resource_types=invalid1&resource_types=invalid2&resource_types=invalid3"
        )
        assert response.status_code == 200

        data = response.json()
        assert data["resource_types"] == []
        assert data["results"] == []  # Should return empty for all invalid types


class TestSearchAPIMultiProjectComprehensive:
    """Comprehensive test class for multi-project search functionality with overlapping resource names"""

    def test_search_across_all_projects_with_overlapping_names(
        self, multi_project_search_test_app
    ):
        """Test searching across all projects when resources have overlapping names"""
        response = multi_project_search_test_app.get("/search?query=user")
        assert response.status_code == 200

        data = response.json()

        # Should find resources from multiple projects
        projects_found = set()
        user_entities = []
        user_features = []
        user_services = []

        for result in data["results"]:
            if "project" in result:
                projects_found.add(result["project"])

            # Collect user-related resources
            if "user" in result.get("name", "").lower():
                if result["type"] == "entity":
                    user_entities.append(result)
                elif result["type"] == "featureView":
                    user_features.append(result)
                elif result["type"] == "featureService":
                    user_services.append(result)

        # Should find resources from project_a and project_b (both have 'user' entities/features)
        assert len(projects_found) >= 2
        assert "project_a" in projects_found
        assert "project_b" in projects_found

        # Should find user entities from both projects with same name but different descriptions
        assert len(user_entities) >= 2
        descriptions = [entity["description"] for entity in user_entities]
        assert any("ride sharing" in desc for desc in descriptions)
        assert any("food delivery" in desc for desc in descriptions)

        # Should find user_features from both projects with same name but different contexts
        assert len(user_features) >= 2
        feature_descriptions = [fv["description"] for fv in user_features]
        assert any("rides" in desc for desc in feature_descriptions)
        assert any("food" in desc for desc in feature_descriptions)

    def test_search_specific_multiple_projects_with_same_resource_names(
        self, multi_project_search_test_app
    ):
        """Test searching in specific projects that have resources with same names"""
        response = multi_project_search_test_app.get(
            "/search?query=user_features&projects=project_a&projects=project_b"
        )
        assert response.status_code == 200

        data = response.json()
        for proj in ["project_a", "project_b"]:
            assert proj in data["projects_searched"]

        # Should find user_features from both specified projects
        user_features_results = [
            r for r in data["results"] if r["name"] == "user_features"
        ]
        assert len(user_features_results) == 2

        # Verify both projects are represented
        projects_in_results = {r["project"] for r in user_features_results}
        assert projects_in_results == {"project_a", "project_b"}

        # Verify different descriptions show they're different resources
        descriptions = {r["description"] for r in user_features_results}
        assert len(descriptions) == 2  # Should have different descriptions

    def test_search_by_domain_tags_across_projects(self, multi_project_search_test_app):
        """Test searching by domain-specific tags across projects"""
        response = multi_project_search_test_app.get("/search?query=transportation")
        assert response.status_code == 200

        data = response.json()

        tag_match_score = 60

        # Should only find resources from project_a (transportation domain)
        project_a_results = [
            r
            for r in data["results"]
            if r.get("project") == "project_a"
            and r.get("match_score") == tag_match_score
        ]

        assert len(project_a_results) > 0
        # Transportation should be specific to project_a based on our test data

        # Test food delivery domain
        response = multi_project_search_test_app.get("/search?query=food_delivery")
        assert response.status_code == 200

        data = response.json()
        project_b_results = [
            r for r in data["results"] if r.get("project") == "project_b"
        ]
        assert len(project_b_results) > 0

    def test_search_common_resource_names_different_contexts(
        self, multi_project_search_test_app
    ):
        """Test searching for resources that have same names but serve different purposes"""
        # Search for "common_analytics" data source which exists in both project_a and project_b
        response = multi_project_search_test_app.get("/search?query=common_analytics")
        assert response.status_code == 200

        data = response.json()

        # Look for unique common_analytics data sources (now prefixed with project names)
        common_analytics_results = [
            r for r in data["results"] if "common_analytics" in r.get("name", "")
        ]

        # Should find project_a_common_analytics and project_b_common_analytics
        project_a_analytics = [
            r
            for r in common_analytics_results
            if r.get("name") == "project_a_common_analytics"
        ]
        project_b_analytics = [
            r
            for r in common_analytics_results
            if r.get("name") == "project_b_common_analytics"
        ]

        assert len(project_a_analytics) == 1, (
            f"Expected 1 project_a_common_analytics, found {len(project_a_analytics)}"
        )
        assert len(project_b_analytics) == 1, (
            f"Expected 1 project_b_common_analytics, found {len(project_b_analytics)}"
        )
        assert len(common_analytics_results) >= 2

        # Should find results from both project_a and project_b
        projects_with_common = {
            r["project"] for r in common_analytics_results if "project" in r
        }
        assert "project_a" in projects_with_common
        assert "project_b" in projects_with_common

    def test_search_unique_resources_by_project(self, multi_project_search_test_app):
        """Test searching for resources that are unique to specific projects"""
        # Search for "restaurant" which should only exist in project_b
        response = multi_project_search_test_app.get("/search?query=restaurant")
        assert response.status_code == 200

        data = response.json()

        restaurant_results = [
            r for r in data["results"] if "restaurant" in r.get("name", "").lower()
        ]
        assert len(restaurant_results) > 0

        # All restaurant results should be from project_b
        for result in restaurant_results:
            if "project" in result:
                assert result["project"] == "project_b"

        # Search for "trip" which should only exist in project_a
        response = multi_project_search_test_app.get("/search?query=trip")
        assert response.status_code == 200

        data = response.json()

        trip_results = [
            r for r in data["results"] if "trip" in r.get("name", "").lower()
        ]
        assert len(trip_results) > 0

        # All trip results should be from project_a
        for result in trip_results:
            if "project" in result:
                assert result["project"] == "project_a"

    def test_search_project_isolation_verification(self, multi_project_search_test_app):
        """Test that project-specific searches properly isolate results"""
        # Search only in project_c
        response = multi_project_search_test_app.get(
            "/search?query=&projects=project_c"
        )
        assert response.status_code == 200

        data = response.json()
        assert data["projects_searched"] == ["project_c"]

        # All results should be from project_c
        for result in data["results"]:
            if "project" in result:
                assert result["project"] == "project_c", (
                    f"Found {result['type']} '{result['name']}' from project '{result['project']}' instead of 'project_c'"
                )

    def test_search_cross_project_resource_comparison(
        self, multi_project_search_test_app
    ):
        """Test comparing same-named resources across different projects"""
        # Search for user_service across projects
        response = multi_project_search_test_app.get("/search?query=user_service")
        assert response.status_code == 200

        data = response.json()

        user_service_results = [
            r for r in data["results"] if r["name"] == "user_service"
        ]
        assert len(user_service_results) >= 2

        # Group by project
        services_by_project = {}
        for service in user_service_results:
            project = service.get("project")
            if project:
                services_by_project[project] = service

        # Should have user_service in both project_a and project_b
        assert "project_a" in services_by_project
        assert "project_b" in services_by_project

        # Verify they have different descriptions (different contexts)
        desc_a = services_by_project["project_a"]["description"]
        desc_b = services_by_project["project_b"]["description"]
        assert desc_a != desc_b
        assert "ride sharing" in desc_a
        assert "food delivery" in desc_b

    def test_search_feature_view_entity_relationships_across_projects(
        self, multi_project_search_test_app
    ):
        """Test that feature views maintain proper entity relationships within each project"""
        response = multi_project_search_test_app.get(
            "/search?query=features&resource_types=feature_views"
        )
        assert response.status_code == 200

        data = response.json()

        # Group feature views by project
        fvs_by_project = {}
        for result in data["results"]:
            if result["type"] == "featureView":
                project = result.get("project")
                if project:
                    if project not in fvs_by_project:
                        fvs_by_project[project] = []
                    fvs_by_project[project].append(result)

        # Each project should have its own feature views
        assert len(fvs_by_project) >= 3

        # Verify project-specific feature views exist
        assert "project_a" in fvs_by_project
        assert "project_b" in fvs_by_project
        assert "project_c" in fvs_by_project

        # Each project should have feature views (project_c only has 1 with "features" in the name)
        for project, fvs in fvs_by_project.items():
            if project == "project_c":
                assert len(fvs) >= 1  # Only sales_features contains "features"
            else:
                assert (
                    len(fvs) >= 2
                )  # project_a and project_b have multiple with "features"

    def test_search_empty_query_cross_project_enumeration(
        self, multi_project_search_test_app
    ):
        """Test empty query returns resources from all projects properly enumerated"""
        response = multi_project_search_test_app.get("/search?query=")
        assert response.status_code == 200

        data = response.json()

        # Should find resources from all three projects
        projects_found = set()
        resource_counts_by_project = {}
        resource_types_by_project = {}

        for result in data["results"]:
            project = result.get("project")
            if project:
                projects_found.add(project)

                # Count resources per project
                if project not in resource_counts_by_project:
                    resource_counts_by_project[project] = 0
                resource_counts_by_project[project] += 1

                # Track resource types per project
                if project not in resource_types_by_project:
                    resource_types_by_project[project] = set()
                resource_types_by_project[project].add(result["type"])

        # Should find all three projects
        assert projects_found == {"project_a", "project_b", "project_c"}

        # Each project should have multiple resources
        for project, count in resource_counts_by_project.items():
            assert count >= 6  # At least entities + feature_views + feature_services

        # Each project should have multiple resource types
        for project, types in resource_types_by_project.items():
            expected_types = {
                "entity",
                "featureView",
                "featureService",
                "dataSource",
                "savedDataset",
                "feature",
            }
            # Should have at least some of the expected types
            assert len(expected_types.intersection(types)) >= 3

    def test_search_project_specific_with_nonexistent_projects(
        self, multi_project_search_test_app
    ):
        """Test searching with mix of existing and non-existing projects"""
        response = multi_project_search_test_app.get(
            "/search?query=user&projects=project_a&projects=nonexistent_project&projects=project_b"
        )
        assert response.status_code == 200

        data = response.json()
        assert len(data["errors"]) == 1
        assert "nonexistent_project" in data["errors"][0]

        for proj in ["project_a", "project_b"]:
            assert proj in data["projects_searched"]

        # Should only find results from existing projects
        projects_with_results = set()
        for result in data["results"]:
            if "project" in result:
                projects_with_results.add(result["project"])

        assert projects_with_results.issubset({"project_a", "project_b"})


class TestSearchAPIPagination:
    """Test class for pagination functionality in search API"""

    @pytest.fixture
    def pagination_responses(self, shared_search_responses, search_test_app):
        """Pre-computed pagination responses to reduce API calls"""
        return {
            "default": shared_search_responses["empty_query"],
            "page1_limit5": shared_search_responses["paginated_basic"],
            "page2_limit3": shared_search_responses["paginated_page2"],
            "large_limit": search_test_app.get(
                "/search?query=&page=1&limit=100"
            ).json(),
            "beyond_results": search_test_app.get(
                "/search?query=&page=999&limit=10"
            ).json(),
            "limit3": search_test_app.get("/search?query=&limit=3").json(),
        }

    def test_search_pagination_basic_functionality(self, pagination_responses):
        """Test basic pagination functionality using shared responses"""

        # Test default values (page=1, limit=50)
        default_data = pagination_responses["default"]
        assert "pagination" in default_data
        pagination = default_data["pagination"]
        assert pagination["page"] == 1
        assert pagination["limit"] == 50
        assert len(default_data["results"]) <= 50
        assert not pagination.get("hasPrevious", False)

        # Test page=1, limit=5
        page1_data = pagination_responses["page1_limit5"]
        pagination = page1_data["pagination"]
        assert pagination["page"] == 1
        assert pagination["limit"] == 5
        assert len(page1_data["results"]) <= 5
        assert not pagination.get("hasPrevious", False)

        # Test page=2, limit=3
        page2_data = pagination_responses["page2_limit3"]
        pagination = page2_data["pagination"]
        assert pagination["page"] == 2
        assert pagination["limit"] == 3
        assert len(page2_data["results"]) <= 3
        if pagination["totalCount"] > 3:
            assert pagination.get("hasPrevious", False)

        # Test large limit
        large_data = pagination_responses["large_limit"]
        pagination = large_data["pagination"]
        assert pagination["page"] == 1
        assert pagination["limit"] == 100
        assert len(large_data["results"]) <= pagination["totalCount"]

        # Test page beyond results
        beyond_data = pagination_responses["beyond_results"]
        pagination = beyond_data["pagination"]
        assert pagination["page"] == 999
        assert pagination["limit"] == 10
        assert len(beyond_data["results"]) == 0
        assert not pagination.get("hasNext", False)

    def test_search_pagination_metadata_comprehensive(
        self, pagination_responses, search_test_app
    ):
        """Comprehensive test for all pagination metadata accuracy using shared responses"""
        # Use limit=3 response for metadata testing
        data = pagination_responses["limit3"]
        total_count = data["pagination"]["totalCount"]
        total_pages = data["pagination"]["totalPages"]
        limit = data["pagination"]["limit"]

        # Verify total_pages calculation: (total + limit - 1) // limit
        expected_pages = (total_count + limit - 1) // limit
        assert total_pages == expected_pages

        # Test pagination metadata structure and types
        pagination = data["pagination"]
        assert isinstance(pagination["page"], int)
        assert isinstance(pagination["limit"], int)
        assert isinstance(pagination["totalCount"], int)
        assert isinstance(pagination["totalPages"], int)
        assert isinstance(pagination["hasNext"], bool)

        page = pagination["page"]
        limit = pagination["limit"]
        total = pagination["totalCount"]

        start = (page - 1) * limit
        end = start + limit

        assert not pagination.get("hasPrevious", False)  # First page has no previous
        expected_has_next = end < total
        assert pagination.get("hasNext", False) == expected_has_next

    @pytest.mark.parametrize(
        "sort_by,sort_order,query,limit",
        [
            ("name", "asc", "", 3),
            ("match_score", "desc", "user", 3),
            ("type", "asc", "", 5),
        ],
    )
    def test_search_pagination_with_sorting(
        self, search_test_app, sort_by, sort_order, query, limit
    ):
        """Test pagination with various sorting parameters"""
        response = search_test_app.get(
            f"/search?query={query}&page=1&limit={limit}&sort_by={sort_by}&sort_order={sort_order}"
        )
        assert response.status_code == 200

        data = response.json()
        results = data["results"]

        if len(results) > 1:
            # Verify results are sorted correctly
            for i in range(len(results) - 1):
                current_value = results[i].get(sort_by, "")
                next_value = results[i + 1].get(sort_by, "")

                if sort_order == "asc":
                    assert current_value <= next_value, (
                        f"Results not sorted ascending by {sort_by}"
                    )
                else:  # desc
                    assert current_value >= next_value, (
                        f"Results not sorted descending by {sort_by}"
                    )

        # Test sorting consistency across pages for name sorting
        if sort_by == "name" and sort_order == "asc":
            # Get second page to verify consistency
            page2_response = search_test_app.get(
                f"/search?query={query}&page=2&limit={limit}&sort_by={sort_by}&sort_order={sort_order}"
            )

            if page2_response.status_code == 200:
                page2_data = page2_response.json()
                page2_results = page2_data["results"]

                if len(results) > 0 and len(page2_results) > 0:
                    # Last item of page 1 should be <= first item of page 2
                    last_page1_name = results[-1]["name"]
                    first_page2_name = page2_results[0]["name"]
                    assert last_page1_name <= first_page2_name

    def test_search_pagination_with_filtering(self, search_test_app):
        """Test pagination with various filtering options"""
        # Test query filtering reduces total count
        response_all = search_test_app.get("/search?query=&limit=10")
        total_all = response_all.json()["pagination"]["totalCount"]

        response_filtered = search_test_app.get("/search?query=user&limit=10")
        total_filtered = response_filtered.json()["pagination"]["totalCount"]

        assert response_all.status_code == 200
        assert response_filtered.status_code == 200
        assert total_filtered <= total_all

        # Test project filtering
        response = search_test_app.get(
            "/search?query=&projects=test_project&page=1&limit=5"
        )
        assert response.status_code == 200

        data = response.json()
        assert "pagination" in data
        assert data["projects_searched"] == ["test_project"]

        # All results should be from test_project
        for result in data["results"]:
            if "project" in result:
                assert result["project"] == "test_project"

        # Test tag filtering
        response = search_test_app.get("/search?query=&tags=team:data&page=1&limit=3")
        assert response.status_code == 200

        data = response.json()
        assert "pagination" in data
        pagination = data["pagination"]
        assert pagination["page"] == 1
        assert pagination["limit"] == 3

        # Test empty results handling
        response = search_test_app.get(
            "/search?query=nonexistent_xyz_123&page=1&limit=10"
        )
        assert response.status_code == 200

        data = response.json()
        pagination = data["pagination"]

        assert not pagination.get("totalCount", False)
        assert not pagination.get("totalPages", False)
        assert not pagination.get("hasNext", False)
        assert not pagination.get("hasPrevious", False)
        assert len(data["results"]) == 0

    def test_search_pagination_boundary_conditions(self, search_test_app):
        """Comprehensive test for pagination boundary conditions and edge cases"""
        # Get total count for boundary calculations
        response = search_test_app.get("/search?query=")
        total_count = response.json()["pagination"]["totalCount"]

        # Test single result per page creates multiple pages
        response = search_test_app.get("/search?query=&page=1&limit=1")
        assert response.status_code == 200
        data = response.json()
        pagination = data["pagination"]

        assert pagination["limit"] == 1
        assert len(data["results"]) <= 1
        if pagination["totalCount"] > 1:
            assert pagination["totalPages"] == pagination["totalCount"]
            assert pagination["hasNext"]

        # Test exact page boundary (when total divisible by limit)
        if total_count >= 4:
            limit = 2 if total_count % 2 == 0 else 3 if total_count % 3 == 0 else 4
            if total_count % limit == 0:
                response = search_test_app.get(f"/search?query=&page=1&limit={limit}")
                data = response.json()
                pagination = data["pagination"]
                expected_pages = total_count // limit
                assert pagination["totalPages"] == expected_pages

        # Test off-by-one boundary conditions
        if total_count > 1:
            limit = total_count - 1
            response = search_test_app.get(f"/search?query=&page=1&limit={limit}")
            data = response.json()
            pagination = data["pagination"]
            assert pagination["totalPages"] == 2
            assert pagination["hasNext"]

        # Test mathematical accuracy of start/end calculations
        test_cases = [(1, 5), (2, 5), (3, 3)]
        for page, limit in test_cases:
            response = search_test_app.get(f"/search?query=&page={page}&limit={limit}")
            assert response.status_code == 200

            data = response.json()
            pagination = data["pagination"]

            expected_start = (page - 1) * limit
            expected_end = expected_start + limit

            assert pagination.get("hasPrevious", False) == (expected_start > 0)
            expected_has_next = expected_end < pagination["totalCount"]
            assert pagination["hasNext"] == expected_has_next

        # Test ceiling division for total pages calculation
        test_limits = [1, 2, 3, 5, 7, 10]
        for limit in test_limits:
            if limit <= total_count:
                response = search_test_app.get(f"/search?query=&limit={limit}")
                data = response.json()
                pagination = data["pagination"]
                expected_pages = (total_count + limit - 1) // limit
                assert pagination["totalPages"] == expected_pages

    def test_search_pagination_navigation_flags(
        self, search_test_app, shared_search_responses
    ):
        """Test has_next and has_previous flags accuracy across different pages"""
        # Test first page has no previous
        data = shared_search_responses["paginated_basic"]
        pagination = data["pagination"]
        assert not pagination.get("hasPrevious", False)
        assert pagination["page"] == 1
        total_pages = pagination.get("totalPages")

        if total_pages > 0:
            response = search_test_app.get(f"/search?query=&page={total_pages}&limit=5")
            data = response.json()
            pagination = data["pagination"]
            assert not pagination.get("hasNext", False)
            assert pagination["page"] == total_pages

        # Test empty results pagination
        response = search_test_app.get(
            "/search?query=impossible_nonexistent_query_xyz_999&page=1&limit=10"
        )
        assert response.status_code == 200
        data = response.json()
        pagination = data["pagination"]
        assert not pagination.get("totalCount", False)
        assert not pagination.get("totalPages", False)
        assert not pagination.get("hasNext", False)
        assert not pagination.get("hasPrevious", False)
        assert len(data["results"]) == 0

    def test_search_pagination_limit_above_maximum(self, search_test_app):
        """Test pagination limit above maximum allowed value (100) returns error"""
        response = search_test_app.get("/search?query=user&limit=150")
        assert response.status_code == 400

        error_data = response.json()
        assert "detail" in error_data
