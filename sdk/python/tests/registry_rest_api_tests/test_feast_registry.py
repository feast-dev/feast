import os

import pytest

credit_scoring_project = "credit_scoring_local"
driver_ranking_project = "driver_ranking"


@pytest.mark.skipif(
    not os.path.exists(os.path.expanduser("~/.kube/config")),
    reason="Kube config not available in this environment",
)
class TestRegistryServerRest:
    # entities tests

    def test_list_entities(self, feast_rest_client):
        response = feast_rest_client.get(f"/entities/?project={credit_scoring_project}")
        assert response.status_code == 200

        data = response.json()

        # Validate presence and structure of 'entities'
        assert "entities" in data
        entities = data["entities"]
        assert isinstance(entities, list)
        assert len(entities) == 4

        expected_entity_names = {"dob_ssn", "zipcode", "__dummy", "loan_id"}
        actual_entity_names = {entity["spec"]["name"] for entity in entities}
        assert expected_entity_names == actual_entity_names

        # Validate pagination section
        assert "pagination" in data
        pagination = data["pagination"]
        assert isinstance(pagination, dict)
        assert pagination.get("totalCount") == 4
        assert pagination.get("totalPages") == 1

    def test_get_entity(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/entities/zipcode/?project={credit_scoring_project}"
        )
        assert response.status_code == 200

        data = response.json()

        # Validate 'spec'
        spec = data["spec"]
        assert spec["name"] == "zipcode"
        assert spec["valueType"] == "INT64"
        assert spec["joinKey"] == "zipcode"
        assert (
            spec["description"]
            == "ZIP code identifier for geographic location-based features"
        )

        # Validate 'meta'
        meta = data["meta"]
        assert "createdTimestamp" in meta
        assert "lastUpdatedTimestamp" in meta

        # Validate 'dataSources'
        data_sources = data["dataSources"]
        assert isinstance(data_sources, list)
        assert len(data_sources) == 1

        ds = data_sources[0]
        assert ds["type"] == "BATCH_FILE"
        assert ds["fileOptions"]["uri"] == "data/zipcode_table.parquet"

        # Validate spec tags
        expected_spec_tags = {
            "join_key": "true",
            "standardized": "true",
            "domain": "geography",
            "cardinality": "high",
            "pii": "false",
            "stability": "stable",
        }
        assert spec["tags"] == expected_spec_tags

        # Validate 'featureDefinition' contains expected entity name
        assert "zipcode" in data["featureDefinition"]

    def test_entities_all(self, feast_rest_client):
        response = feast_rest_client.get("/entities/all")
        assert response.status_code == 200

        data = response.json()

        assert "entities" in data
        assert "pagination" in data

        # Validate pagination
        pagination = data["pagination"]
        assert pagination["page"] == 1
        assert pagination["limit"] == 50
        assert pagination["totalCount"] == len(data["entities"])
        assert pagination["totalPages"] >= 1

        entities = data["entities"]
        assert len(entities) >= 1

        # Validate each entity
        for entity in entities:
            assert "spec" in entity
            assert "meta" in entity
            assert "project" in entity

            spec = entity["spec"]
            assert "name" in spec
            assert "joinKey" in spec

            if "valueType" in spec:
                assert isinstance(spec["valueType"], str)
            if "description" in spec:
                assert isinstance(spec["description"], str)

            meta = entity["meta"]
            assert "createdTimestamp" in meta
            assert "lastUpdatedTimestamp" in meta

            # Project should be a known format
            assert isinstance(entity["project"], str)
            assert entity["project"] in ["credit_scoring_local", "driver_ranking"]

    # Data sources tests

    def test_list_data_sources(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/data_sources/?project={credit_scoring_project}"
        )
        assert response.status_code == 200
        data = response.json()
        # Validate top-level keys
        assert "dataSources" in data
        assert "pagination" in data

        data_sources = data["dataSources"]
        assert len(data_sources) == 5

        # Validate pagination section
        pagination = data["pagination"]
        assert pagination["totalCount"] == 5
        assert pagination["totalPages"] == 1

    def test_get_data_sources(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/data_sources/Zipcode source/?project={credit_scoring_project}"
        )
        assert response.status_code == 200
        data = response.json()

        assert data["type"] == "BATCH_FILE"
        assert data["name"] == "Zipcode source"

        assert "featureDefinition" in data
        assert "FileSource" in data["featureDefinition"]
        assert "Zipcode source" in data["featureDefinition"]
        assert "event_timestamp" in data["featureDefinition"]

    def test_data_sources_all(self, feast_rest_client):
        response = feast_rest_client.get("/data_sources/all")
        assert response.status_code == 200
        data = response.json()

        data_sources = data["dataSources"]
        assert len(data_sources) >= 1

        for ds in data_sources:
            if ds["type"] in ("BATCH_FILE", "REQUEST_SOURCE"):
                assert ds["project"] in (credit_scoring_project, driver_ranking_project)
            else:
                continue

        pagination = data.get("pagination", {})
        assert pagination.get("page") == 1
        assert pagination.get("limit") >= len(data_sources)
        assert pagination.get("totalCount") == len(data_sources)
        assert pagination.get("totalPages") == 1

    # Feature services
    def test_list_feature_services(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/feature_services/?project={driver_ranking_project}"
        )
        assert response.status_code == 200

        data = response.json()
        feature_services = data.get("featureServices", [])
        assert len(feature_services) == 3, (
            f"Expected 3 feature services, got {len(feature_services)}"
        )

        for fs in feature_services:
            features = fs["spec"].get("features", [])
            for feat in features:
                batch_source = feat.get("batchSource")
                if batch_source:
                    assert batch_source["type"] == "BATCH_FILE"

    def test_feature_services_all(self, feast_rest_client):
        response = feast_rest_client.get("/feature_services/all")
        assert response.status_code == 200

        data = response.json()
        feature_services = data.get("featureServices", [])

        assert len(feature_services) >= 1

        for fs in feature_services:
            assert fs.get("project") in [credit_scoring_project, driver_ranking_project]
            spec = fs.get("spec", {})
            features = spec.get("features", [])

            for feature in features:
                batch_source = feature.get("batchSource")
                if batch_source:
                    assert batch_source.get("type") == "BATCH_FILE"

    def test_get_feature_services(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/feature_services/driver_activity_v2/?project={driver_ranking_project}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["spec"]["name"] == "driver_activity_v2"

        # Validate each feature block
        for feature in data["spec"].get("features", []):
            batch_source = feature.get("batchSource")
            if batch_source:
                assert batch_source.get("type") == "BATCH_FILE"

    # Feature views
    def test_list_feature_views(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/feature_views/?project={credit_scoring_project}"
        )
        assert response.status_code == 200

        data = response.json()

        # Assert the number of feature views
        assert len(data["featureViews"]) == 8

        # Validate pagination block presence and values
        pagination = data.get("pagination")
        assert pagination is not None
        assert pagination.get("totalCount") == 8
        assert pagination.get("totalPages") == 1

    def test_get_feature_view(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/feature_views/credit_history/?project={credit_scoring_project}"
        )
        assert response.status_code == 200
        data = response.json()
        assert data.get("type") == "featureView"
        spec = data["spec"]
        assert spec.get("name") == "credit_history"
        features = spec.get("features", [])
        assert len(features) > 0

    def test_feature_views_all(self, feast_rest_client):
        response = feast_rest_client.get("/feature_views/all")
        assert response.status_code == 200

        data = response.json()
        feature_views = data.get("featureViews")
        assert isinstance(feature_views, list), "Expected 'featureViews' to be a list"
        assert len(feature_views) > 0

        pagination = data.get("pagination")
        assert pagination.get("page") == 1
        assert pagination.get("limit") == 50
        assert pagination.get("totalCount") == len(feature_views)
        assert pagination.get("totalPages") == 1

    # features tests

    def test_list_features(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/features/?project={credit_scoring_project}&include_relationships=true"
        )
        assert response.status_code == 200

        data = response.json()

        features = data.get("features")
        assert isinstance(features, list)
        assert len(features) == 31

        for feature in features:
            assert "name" in feature
            assert "featureView" in feature
            assert "type" in feature
            assert isinstance(feature["name"], str)
            assert isinstance(feature["featureView"], str)
            assert isinstance(feature["type"], str)

        # Validate pagination metadata
        pagination = data.get("pagination")
        assert isinstance(pagination, dict)
        assert pagination.get("totalCount") == 31
        assert pagination.get("totalPages") == 1

    def test_get_feature(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/features/zipcode_features/city/?project={credit_scoring_project}&include_relationships=false"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "city"
        assert data["featureView"] == "zipcode_features"
        assert data["type"] == "String"
        assert data["description"] == "City name for the ZIP code"

    def test_features_all(self, feast_rest_client):
        response = feast_rest_client.get("/features/all")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data["features"], list)
        assert len(data["features"]) > 0

        # Validate required fields in each feature
        for feature in data["features"]:
            assert "name" in feature
            assert "featureView" in feature
            assert "type" in feature
            assert "project" in feature
            assert isinstance(feature["name"], str)
            assert isinstance(feature["featureView"], str)
            assert isinstance(feature["type"], str)
            assert isinstance(feature["project"], str)

        expected_projects = {"credit_scoring_local", "driver_ranking"}
        actual_projects = set(f["project"] for f in data["features"])
        assert expected_projects.issubset(actual_projects)

        # Validate pagination structure
        pagination = data.get("pagination")
        assert pagination is not None
        assert pagination.get("page") == 1
        assert pagination.get("limit") == 50
        assert pagination.get("totalCount") == len(data["features"])
        assert pagination.get("totalPages") == 1

    # Projects tests

    def test_get_project_by_name(self, feast_rest_client):
        response = feast_rest_client.get(f"/projects/{credit_scoring_project}")
        assert response.status_code == 200
        data = response.json()
        assert data["spec"]["name"] == credit_scoring_project

    def test_get_projects_list(self, feast_rest_client):
        response = feast_rest_client.get("/projects")
        assert response.status_code == 200
        data = response.json()
        assert len(data["projects"]) == 2
        for project in data["projects"]:
            assert project["spec"]["name"] in [
                credit_scoring_project,
                driver_ranking_project,
            ]

    # lineage

    def test_get_registry_lineage(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/lineage/registry?project={credit_scoring_project}"
        )
        assert response.status_code == 200
        data = response.json()

        assert "relationships" in data
        assert "indirect_relationships" in data
        assert "relationships_pagination" in data
        assert "indirect_relationships_pagination" in data

        # Validate relationships pagination
        rel_page = data["relationships_pagination"]
        assert rel_page["totalCount"] == 71
        assert rel_page["totalPages"] == 1

        # Validate indirect relationships pagination
        indirect_page = data["indirect_relationships_pagination"]
        assert indirect_page["totalCount"] == 154
        assert indirect_page["totalPages"] == 1

    def test_get_lineage_complete(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/lineage/complete?project={credit_scoring_project}"
        )
        assert response.status_code == 200
        data = response.json()

        assert data.get("project") == "credit_scoring_local"
        assert "objects" in data
        objects = data["objects"]

        # Validate entities exist
        entities = objects.get("entities", [])
        assert len(entities) > 0

        data_sources = objects.get("dataSources", [])
        data_source_types = {ds.get("type") for ds in data_sources}
        assert "BATCH_FILE" in data_source_types
        assert "REQUEST_SOURCE" in data_source_types

        #  Validate pagination structure
        pagination = data.get("pagination", {})
        assert isinstance(pagination, dict)

        # Expected pagination keys and values
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

            if not page_info:
                continue

            assert isinstance(page_info.get("totalCount"), int), (
                f"'totalCount' missing or invalid for '{key}'"
            )
            assert isinstance(page_info.get("totalPages"), int), (
                f"'totalPages' missing or invalid for '{key}'"
            )

    def test_get_registry_lineage_all(self, feast_rest_client):
        response = feast_rest_client.get("/lineage/registry/all")
        assert response.status_code == 200
        data = response.json()
        assert "relationships" in data
        relationships = data["relationships"]
        assert isinstance(relationships, list), "'relationships' should be a list"
        assert len(relationships) > 0, "No relationships found"

    def test_get_registry_complete_all(self, feast_rest_client):
        response = feast_rest_client.get("/lineage/complete/all")
        assert response.status_code == 200
        data = response.json()
        assert "projects" in data
        assert len(data["projects"]) > 0

        project_names = [project["project"] for project in data.get("projects", [])]

        # Assert the expected project name is present
        assert "credit_scoring_local" in project_names

    def test_get_lineage_object_path(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/lineage/objects/entity/dob_ssn?project={credit_scoring_project}"
        )
        assert response.status_code == 200
        data = response.json()

        assert isinstance(data["relationships"], list)
        assert len(data["relationships"]) == 2

        relationship = data["relationships"][0]
        assert relationship["source"]["type"] == "entity"
        assert relationship["source"]["name"] == "dob_ssn"

        # Validate pagination block
        assert "pagination" in data
        pagination = data["pagination"]
        assert pagination.get("totalCount") == 2
        assert pagination.get("totalPages") == 1

    # Datasets tests

    def test_list_saved_datasets(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/saved_datasets?project={credit_scoring_project}&include_relationships=false"
        )
        assert response.status_code == 200
        data = response.json()
        assert "saved_datasets" in data
        assert len(data["saved_datasets"]) > 0

        expected_names = [
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

        # Extract all names from spec
        actual_names = [ds["spec"]["name"] for ds in data["saved_datasets"]]

        # Verify size
        assert len(actual_names) == len(expected_names), (
            f"Size mismatch: {len(actual_names)} != {len(expected_names)}"
        )

        # Verify names match exactly
        assert actual_names == expected_names, (
            f"Names mismatch:\nExpected: {expected_names}\nActual: {actual_names}"
        )

        # Validate pagination block
        assert "pagination" in data
        pagination = data["pagination"]
        assert pagination.get("totalCount") == 9
        assert pagination.get("totalPages") == 1

    def test_get_all_saved_datasets(self, feast_rest_client):
        response = feast_rest_client.get(
            "/saved_datasets/all?allow_cache=true&page=1&limit=50&sort_order=asc&include_relationships=false"
        )
        assert response.status_code == 200
        data = response.json()

        assert "savedDatasets" in data
        assert len(data["savedDatasets"]) > 0

        expected_names = [
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

        # Extract all names from spec
        actual_names = [ds["spec"]["name"] for ds in data["savedDatasets"]]

        # Verify size
        assert len(actual_names) == len(expected_names), (
            f"Size mismatch: {len(actual_names)} != {len(expected_names)}"
        )

        # Verify names match exactly
        assert actual_names == expected_names, (
            f"Names mismatch:\nExpected: {expected_names}\nActual: {actual_names}"
        )

        # Validate pagination block
        assert "pagination" in data
        pagination = data["pagination"]
        assert pagination.get("page") == 1
        assert pagination.get("limit") == 50
        assert pagination.get("totalCount") == 9
        assert pagination.get("totalPages") == 1

    def test_get_saved_datasets_by_name(self, feast_rest_client):
        dataset_name = "comprehensive_credit_dataset_v1"
        response = feast_rest_client.get(
            f"/saved_datasets/{dataset_name}?project=credit_scoring_local&include_relationships=false"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["spec"]["name"] == dataset_name
        assert "features" in data["spec"]
        assert len(data["spec"]["features"]) == 6

    # Permissions tests

    def test_get_permission_by_name(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/permissions/feast_admin_permission?project={credit_scoring_project}&include_relationships=false"
        )
        assert response.status_code == 200

    def test_list_permissions(self, feast_rest_client):
        response = feast_rest_client.get(
            f"/permissions?project={credit_scoring_project}&include_relationships=false"
        )
        assert response.status_code == 200
        data = response.json()
        assert "permissions" in data
        assert "pagination" in data

        expected_permission_names = ["feast_admin_permission", "feast_user_permission"]
        # Extract all names from spec
        actual_names = [ds["spec"]["name"] for ds in data["permissions"]]

        # Verify size
        assert len(actual_names) == len(expected_permission_names), (
            f"Size mismatch: {len(actual_names)} != {len(expected_permission_names)}"
        )

        # Verify names match exactly
        for name in expected_permission_names:
            assert name in actual_names

        pagination = data.get("pagination")
        assert pagination.get("totalCount") == 2
        assert pagination.get("totalPages") == 1
