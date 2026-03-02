"""
Feast Registry REST API Tests

This module contains comprehensive tests for the Feast Registry REST API by deploying projects credit_scoring_local and driver_ranking.
"""

import os
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest


# Test Configuration Constants
@dataclass(frozen=True)
class RegistryTestConfig:
    """Configuration constants for registry REST API tests."""

    CREDIT_SCORING_PROJECT = "credit_scoring_local"
    DRIVER_RANKING_PROJECT = "driver_ranking"

    # Expected counts
    CREDIT_SCORING_ENTITIES_COUNT = 4
    CREDIT_SCORING_DATA_SOURCES_COUNT = 5
    CREDIT_SCORING_FEATURE_VIEWS_COUNT = 8
    CREDIT_SCORING_FEATURES_COUNT = 31
    DRIVER_RANKING_FEATURE_SERVICES_COUNT = 3
    SAVED_DATASETS_COUNT = 9
    PERMISSIONS_COUNT = 2

    # Expected data
    ENTITY_NAMES = {"dob_ssn", "zipcode", "__dummy", "loan_id"}
    PROJECT_NAMES = {CREDIT_SCORING_PROJECT, DRIVER_RANKING_PROJECT}

    SAVED_DATASET_NAMES = [
        "credit_score_training_v1",
        "credit_history_analysis_v1",
        "demographics_profile_v1",
        "comprehensive_credit_dataset_v1",
        "credit_history_service_v1",
        "demographics_service_v1",
        "location_intelligence_service_v1",
        "customer_profile_service_v1",
        "basic_underwriting_service_v1",
    ]

    PERMISSION_NAMES = ["feast_admin_permission", "feast_user_permission"]

    # Zipcode entity expected data
    ZIPCODE_SPEC_TAGS = {
        "join_key": "true",
        "standardized": "true",
        "domain": "geography",
        "cardinality": "high",
        "pii": "false",
        "stability": "stable",
    }


class APITestHelpers:
    """Helper methods for common API response validations."""

    @staticmethod
    def validate_response_success(response) -> Dict[str, Any]:
        """Validate response is successful and return JSON data."""
        assert response.status_code == 200
        return response.json()

    @staticmethod
    def validate_pagination(
        data: Dict[str, Any], expected_total: int, expected_pages: int = 1
    ) -> None:
        """Validate pagination structure and values."""
        assert "pagination" in data
        pagination = data["pagination"]
        assert isinstance(pagination, dict)
        assert pagination.get("totalCount") == expected_total
        assert pagination.get("totalPages") == expected_pages

    @staticmethod
    def validate_pagination_all_endpoint(data: Dict[str, Any], items_key: str) -> None:
        """Validate pagination for 'all' endpoints."""
        pagination = data.get("pagination")
        assert pagination is not None
        assert pagination.get("page") == 1
        assert pagination.get("limit") == 50
        assert pagination.get("totalCount") == len(data[items_key])
        assert pagination.get("totalPages") == 1

    @staticmethod
    def validate_entity_structure(entity: Dict[str, Any]) -> None:
        """Validate common entity structure."""
        required_keys = ["spec", "meta", "project"]
        for key in required_keys:
            assert key in entity

        spec = entity["spec"]
        assert "name" in spec
        assert "joinKey" in spec

        meta = entity["meta"]
        assert "createdTimestamp" in meta
        assert "lastUpdatedTimestamp" in meta

        assert isinstance(entity["project"], str)
        assert entity["project"] in RegistryTestConfig.PROJECT_NAMES

    @staticmethod
    def validate_feature_structure(feature: Dict[str, Any]) -> None:
        """Validate common feature structure."""
        required_fields = ["name", "featureView", "type"]
        for field in required_fields:
            assert field in feature
            assert isinstance(feature[field], str)

    @staticmethod
    def validate_names_match(
        actual_names: List[str], expected_names: List[str]
    ) -> None:
        """Validate that actual names match expected names exactly."""
        assert len(actual_names) == len(expected_names), (
            f"Size mismatch: {len(actual_names)} != {len(expected_names)}"
        )
        assert set(actual_names) == set(expected_names), (
            f"Names mismatch:\nExpected: {expected_names}\nActual: {actual_names}"
        )

    @staticmethod
    def validate_batch_source(batch_source: Dict[str, Any]) -> None:
        """Validate batch source structure."""
        if batch_source:
            assert batch_source.get("type") == "BATCH_FILE"


@pytest.mark.integration
@pytest.mark.skipif(
    not os.path.exists(os.path.expanduser("~/.kube/config")),
    reason="Kube config not available in this environment",
)
class TestRegistryServerRest:
    """Test suite for Feast Registry REST API endpoints."""

    # Entity Tests
    def test_list_entities(self, feast_rest_client):
        """Test listing entities for a specific project."""
        response = feast_rest_client.get(
            f"/entities/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        # Validate entities structure
        assert "entities" in data
        entities = data["entities"]
        assert isinstance(entities, list)
        assert len(entities) == RegistryTestConfig.CREDIT_SCORING_ENTITIES_COUNT

        # Validate entity names
        actual_entity_names = {entity["spec"]["name"] for entity in entities}
        assert actual_entity_names == RegistryTestConfig.ENTITY_NAMES

        # Validate pagination
        APITestHelpers.validate_pagination(
            data, RegistryTestConfig.CREDIT_SCORING_ENTITIES_COUNT
        )

    def test_get_entity(self, feast_rest_client):
        """Test getting a specific entity with detailed validation."""
        response = feast_rest_client.get(
            f"/entities/zipcode/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        # Validate spec
        spec = data["spec"]
        assert spec["name"] == "zipcode"
        assert spec["valueType"] == "INT64"
        assert spec["joinKey"] == "zipcode"
        assert (
            spec["description"]
            == "ZIP code identifier for geographic location-based features"
        )
        assert spec["tags"] == RegistryTestConfig.ZIPCODE_SPEC_TAGS

        # Validate meta
        meta = data["meta"]
        assert "createdTimestamp" in meta
        assert "lastUpdatedTimestamp" in meta

        # Validate data sources
        data_sources = data["dataSources"]
        assert isinstance(data_sources, list)
        assert len(data_sources) == 1
        ds = data_sources[0]
        assert ds["type"] == "BATCH_FILE"
        assert ds["fileOptions"]["uri"] == "data/zipcode_table.parquet"

        # Validate feature definition
        assert "zipcode" in data["featureDefinition"]

    def test_entities_all(self, feast_rest_client):
        """Test listing all entities across projects."""
        response = feast_rest_client.get("/entities/all")
        data = APITestHelpers.validate_response_success(response)

        assert "entities" in data
        entities = data["entities"]
        assert len(entities) >= 1

        # Validate each entity structure
        for entity in entities:
            APITestHelpers.validate_entity_structure(entity)

        APITestHelpers.validate_pagination_all_endpoint(data, "entities")

    # Data Source Tests
    def test_list_data_sources(self, feast_rest_client):
        """Test listing data sources for a specific project."""
        response = feast_rest_client.get(
            f"/data_sources/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        assert "dataSources" in data
        data_sources = data["dataSources"]
        assert len(data_sources) == RegistryTestConfig.CREDIT_SCORING_DATA_SOURCES_COUNT

        APITestHelpers.validate_pagination(
            data, RegistryTestConfig.CREDIT_SCORING_DATA_SOURCES_COUNT
        )

    def test_get_data_sources(self, feast_rest_client):
        """Test getting a specific data source."""
        response = feast_rest_client.get(
            f"/data_sources/Zipcode source/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        assert data["type"] == "BATCH_FILE"
        assert data["name"] == "Zipcode source"

        # Validate feature definition content
        feature_def = data["featureDefinition"]
        expected_content = ["FileSource", "Zipcode source", "event_timestamp"]
        for content in expected_content:
            assert content in feature_def

    def test_data_sources_all(self, feast_rest_client):
        """Test listing all data sources across projects."""
        response = feast_rest_client.get("/data_sources/all")
        data = APITestHelpers.validate_response_success(response)

        data_sources = data["dataSources"]
        assert len(data_sources) >= 1

        # Validate project associations for relevant data source types
        for ds in data_sources:
            if ds["type"] in ("BATCH_FILE", "REQUEST_SOURCE"):
                assert ds["project"] in RegistryTestConfig.PROJECT_NAMES

        pagination = data.get("pagination", {})
        assert pagination.get("page") == 1
        assert pagination.get("limit") >= len(data_sources)
        assert pagination.get("totalCount") >= len(data_sources)
        assert "totalPages" in pagination

    # Feature Service Tests
    def test_list_feature_services(self, feast_rest_client):
        """Test listing feature services for a specific project."""
        response = feast_rest_client.get(
            f"/feature_services/?project={RegistryTestConfig.DRIVER_RANKING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        feature_services = data.get("featureServices", [])
        assert (
            len(feature_services)
            == RegistryTestConfig.DRIVER_RANKING_FEATURE_SERVICES_COUNT
        )

        # Validate batch sources in features
        for fs in feature_services:
            features = fs["spec"].get("features", [])
            for feat in features:
                APITestHelpers.validate_batch_source(feat.get("batchSource"))

    def test_feature_services_all(self, feast_rest_client):
        """Test listing all feature services across projects."""
        response = feast_rest_client.get("/feature_services/all")
        data = APITestHelpers.validate_response_success(response)

        feature_services = data.get("featureServices", [])
        assert len(feature_services) >= 1

        for fs in feature_services:
            assert fs.get("project") in RegistryTestConfig.PROJECT_NAMES

            # Validate features structure
            spec = fs.get("spec", {})
            features = spec.get("features", [])
            for feature in features:
                APITestHelpers.validate_batch_source(feature.get("batchSource"))

    def test_get_feature_services(self, feast_rest_client):
        """Test getting a specific feature service."""
        response = feast_rest_client.get(
            f"/feature_services/driver_activity_v2/?project={RegistryTestConfig.DRIVER_RANKING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        assert data["spec"]["name"] == "driver_activity_v2"

        # Validate each feature block
        for feature in data["spec"].get("features", []):
            APITestHelpers.validate_batch_source(feature.get("batchSource"))

    # Feature View Tests
    def test_list_feature_views(self, feast_rest_client):
        """Test listing feature views for a specific project."""
        response = feast_rest_client.get(
            f"/feature_views/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        assert (
            len(data["featureViews"])
            == RegistryTestConfig.CREDIT_SCORING_FEATURE_VIEWS_COUNT
        )
        APITestHelpers.validate_pagination(
            data, RegistryTestConfig.CREDIT_SCORING_FEATURE_VIEWS_COUNT
        )

    def test_get_feature_view(self, feast_rest_client):
        """Test getting a specific feature view."""
        response = feast_rest_client.get(
            f"/feature_views/credit_history/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        assert data.get("type") == "featureView"
        spec = data["spec"]
        assert spec.get("name") == "credit_history"
        assert len(spec.get("features", [])) > 0

    def test_feature_views_all(self, feast_rest_client):
        """Test listing all feature views across projects."""
        response = feast_rest_client.get("/feature_views/all")
        data = APITestHelpers.validate_response_success(response)

        feature_views = data.get("featureViews")
        assert isinstance(feature_views, list), "Expected 'featureViews' to be a list"
        assert len(feature_views) > 0

        APITestHelpers.validate_pagination_all_endpoint(data, "featureViews")

    # Feature Tests
    def test_list_features(self, feast_rest_client):
        """Test listing features for a specific project."""
        response = feast_rest_client.get(
            f"/features/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}&include_relationships=true"
        )
        data = APITestHelpers.validate_response_success(response)

        features = data.get("features")
        assert isinstance(features, list)
        assert len(features) == RegistryTestConfig.CREDIT_SCORING_FEATURES_COUNT

        # Validate each feature structure
        for feature in features:
            APITestHelpers.validate_feature_structure(feature)

        APITestHelpers.validate_pagination(
            data, RegistryTestConfig.CREDIT_SCORING_FEATURES_COUNT
        )

    def test_get_feature(self, feast_rest_client):
        """Test getting a specific feature."""
        response = feast_rest_client.get(
            f"/features/zipcode_features/city/?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}&include_relationships=false"
        )
        data = APITestHelpers.validate_response_success(response)

        assert data["name"] == "city"
        assert data["featureView"] == "zipcode_features"
        assert data["type"] == "String"
        assert data["description"] == "City name for the ZIP code"

    def test_features_all(self, feast_rest_client):
        """Test listing all features across projects."""
        response = feast_rest_client.get("/features/all")
        data = APITestHelpers.validate_response_success(response)

        features = data["features"]
        assert isinstance(features, list)
        assert len(features) > 0

        # Validate required fields in each feature
        for feature in features:
            APITestHelpers.validate_feature_structure(feature)
            assert "project" in feature
            assert isinstance(feature["project"], str)

        # Validate expected projects are present
        actual_projects = set(f["project"] for f in features)
        assert RegistryTestConfig.PROJECT_NAMES.issubset(actual_projects)

        APITestHelpers.validate_pagination_all_endpoint(data, "features")

    # Project Tests
    @pytest.mark.parametrize(
        "project_name",
        [
            RegistryTestConfig.CREDIT_SCORING_PROJECT,
            RegistryTestConfig.DRIVER_RANKING_PROJECT,
        ],
    )
    def test_get_project_by_name(self, feast_rest_client, project_name):
        """Test getting a project by name."""
        response = feast_rest_client.get(f"/projects/{project_name}")
        data = APITestHelpers.validate_response_success(response)
        assert data["spec"]["name"] == project_name

    def test_get_projects_list(self, feast_rest_client):
        """Test listing all projects."""
        response = feast_rest_client.get("/projects")
        data = APITestHelpers.validate_response_success(response)

        projects = data["projects"]
        assert len(projects) == 2

        actual_project_names = [project["spec"]["name"] for project in projects]
        assert set(actual_project_names) == RegistryTestConfig.PROJECT_NAMES

    # Lineage Tests
    def test_get_registry_lineage(self, feast_rest_client):
        """Test getting registry lineage for a specific project."""
        response = feast_rest_client.get(
            f"/lineage/registry?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        required_keys = [
            "relationships",
            "indirect_relationships",
            "relationships_pagination",
            "indirect_relationships_pagination",
        ]
        for key in required_keys:
            assert key in data

        # Validate specific pagination counts (these are test-specific)
        assert data["relationships_pagination"]["totalCount"] == 71
        assert data["relationships_pagination"]["totalPages"] == 1
        assert data["indirect_relationships_pagination"]["totalCount"] == 154
        assert data["indirect_relationships_pagination"]["totalPages"] == 1

    def test_get_lineage_complete(self, feast_rest_client):
        """Test getting complete lineage for a specific project."""
        response = feast_rest_client.get(
            f"/lineage/complete?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        assert data.get("project") == RegistryTestConfig.CREDIT_SCORING_PROJECT
        assert "objects" in data

        objects = data["objects"]

        # Validate entities exist
        entities = objects.get("entities", [])
        assert len(entities) > 0

        # Validate data source types
        data_sources = objects.get("dataSources", [])
        data_source_types = {ds.get("type") for ds in data_sources}
        expected_types = {"BATCH_FILE", "REQUEST_SOURCE"}
        assert expected_types.issubset(data_source_types)

        # Validate pagination structure
        self._validate_lineage_pagination(data.get("pagination", {}))

    def _validate_lineage_pagination(self, pagination: Dict[str, Any]) -> None:
        """Helper method to validate lineage pagination structure."""
        assert isinstance(pagination, dict)

        expected_keys = [
            "entities",
            "dataSources",
            "featureViews",
            "featureServices",
            "features",
            "relationships",
            "indirectRelationships",
        ]

        for key in expected_keys:
            assert key in pagination, f"Missing pagination entry for '{key}'"
            page_info = pagination[key]

            if page_info:  # Skip empty pagination info
                assert isinstance(page_info.get("totalCount"), int)
                assert isinstance(page_info.get("totalPages"), int)

    def test_get_registry_lineage_all(self, feast_rest_client):
        """Test getting all registry lineage across projects."""
        response = feast_rest_client.get("/lineage/registry/all")
        data = APITestHelpers.validate_response_success(response)

        assert "relationships" in data
        relationships = data["relationships"]
        assert isinstance(relationships, list), "'relationships' should be a list"
        assert len(relationships) > 0, "No relationships found"

    def test_get_registry_complete_all(self, feast_rest_client):
        """Test getting complete registry information across all projects."""
        response = feast_rest_client.get("/lineage/complete/all")
        data = APITestHelpers.validate_response_success(response)

        assert "projects" in data
        assert len(data["projects"]) > 0

        project_names = [project["project"] for project in data.get("projects", [])]
        assert RegistryTestConfig.CREDIT_SCORING_PROJECT in project_names

    def test_get_lineage_object_path(self, feast_rest_client):
        """Test getting lineage for a specific object."""
        response = feast_rest_client.get(
            f"/lineage/objects/entity/dob_ssn?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}"
        )
        data = APITestHelpers.validate_response_success(response)

        relationships = data["relationships"]
        assert isinstance(relationships, list)
        assert len(relationships) == 2

        relationship = relationships[0]
        assert relationship["source"]["type"] == "entity"
        assert relationship["source"]["name"] == "dob_ssn"

        APITestHelpers.validate_pagination(data, 2)

    # Saved Dataset Tests
    @pytest.mark.parametrize(
        "endpoint,key",
        [
            ("/saved_datasets", "savedDatasets"),
            ("/saved_datasets/all", "savedDatasets"),
        ],
    )
    def test_saved_datasets_endpoints(self, feast_rest_client, endpoint, key):
        """Test saved datasets endpoints with parameterization."""
        if endpoint == "/saved_datasets":
            url = f"{endpoint}?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}&include_relationships=false"
        else:
            url = f"{endpoint}?allow_cache=true&page=1&limit=50&sort_order=asc&include_relationships=false"

        response = feast_rest_client.get(url)
        data = APITestHelpers.validate_response_success(response)

        assert key in data
        saved_datasets = data[key]
        assert len(saved_datasets) > 0

        # Extract and validate names
        actual_names = [ds["spec"]["name"] for ds in saved_datasets]
        APITestHelpers.validate_names_match(
            actual_names, RegistryTestConfig.SAVED_DATASET_NAMES
        )

        # Validate pagination
        APITestHelpers.validate_pagination(
            data, RegistryTestConfig.SAVED_DATASETS_COUNT
        )
        if endpoint == "/saved_datasets/all":
            assert data["pagination"]["page"] == 1
            assert data["pagination"]["limit"] == 50

    def test_get_saved_datasets_by_name(self, feast_rest_client):
        """Test getting a specific saved dataset by name."""
        dataset_name = "comprehensive_credit_dataset_v1"
        response = feast_rest_client.get(
            f"/saved_datasets/{dataset_name}?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}&include_relationships=false"
        )
        data = APITestHelpers.validate_response_success(response)

        assert data["spec"]["name"] == dataset_name
        assert "features" in data["spec"]
        assert len(data["spec"]["features"]) == 6

    # Permission Tests
    def test_get_permission_by_name(self, feast_rest_client):
        """Test getting a specific permission by name."""
        response = feast_rest_client.get(
            f"/permissions/feast_admin_permission?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}&include_relationships=false"
        )
        APITestHelpers.validate_response_success(response)

    def test_list_permissions(self, feast_rest_client):
        """Test listing permissions for a specific project."""
        response = feast_rest_client.get(
            f"/permissions?project={RegistryTestConfig.CREDIT_SCORING_PROJECT}&include_relationships=false"
        )
        data = APITestHelpers.validate_response_success(response)

        assert "permissions" in data

        # Extract and validate names
        actual_names = [ds["spec"]["name"] for ds in data["permissions"]]
        assert len(actual_names) == len(RegistryTestConfig.PERMISSION_NAMES)

        for name in RegistryTestConfig.PERMISSION_NAMES:
            assert name in actual_names

        APITestHelpers.validate_pagination(data, RegistryTestConfig.PERMISSIONS_COUNT)
