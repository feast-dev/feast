"""
Unit tests for dbt manifest parser.
"""

import json
import pytest
from pathlib import Path

from feast.dbt.parser import DbtManifestParser, DbtModel, DbtColumn


@pytest.fixture
def sample_manifest(tmp_path):
    """Create a sample dbt manifest.json for testing."""
    manifest = {
        "metadata": {
            "dbt_version": "1.5.0",
            "project_name": "test_project",
            "generated_at": "2024-01-10T00:00:00Z",
        },
        "nodes": {
            "model.test_project.driver_stats": {
                "name": "driver_stats",
                "unique_id": "model.test_project.driver_stats",
                "resource_type": "model",
                "database": "my_database",
                "schema": "analytics",
                "alias": "driver_stats",
                "description": "Driver statistics aggregated hourly",
                "columns": {
                    "driver_id": {
                        "name": "driver_id",
                        "description": "Unique driver identifier",
                        "data_type": "INT64",
                        "tags": ["entity"],
                        "meta": {},
                    },
                    "event_timestamp": {
                        "name": "event_timestamp",
                        "description": "Event timestamp",
                        "data_type": "TIMESTAMP",
                        "tags": [],
                        "meta": {},
                    },
                    "trip_count": {
                        "name": "trip_count",
                        "description": "Number of trips",
                        "data_type": "INT64",
                        "tags": ["feature"],
                        "meta": {},
                    },
                    "avg_rating": {
                        "name": "avg_rating",
                        "description": "Average driver rating",
                        "data_type": "FLOAT64",
                        "tags": ["feature"],
                        "meta": {},
                    },
                },
                "tags": ["feast", "ml"],
                "meta": {"owner": "data-team"},
                "depends_on": {"nodes": ["source.test_project.raw_trips"]},
            },
            "model.test_project.customer_stats": {
                "name": "customer_stats",
                "unique_id": "model.test_project.customer_stats",
                "resource_type": "model",
                "database": "my_database",
                "schema": "analytics",
                "alias": "customer_stats",
                "description": "Customer statistics",
                "columns": {
                    "customer_id": {
                        "name": "customer_id",
                        "description": "Unique customer ID",
                        "data_type": "STRING",
                        "tags": [],
                        "meta": {},
                    },
                    "event_timestamp": {
                        "name": "event_timestamp",
                        "description": "Event timestamp",
                        "data_type": "TIMESTAMP",
                        "tags": [],
                        "meta": {},
                    },
                    "order_count": {
                        "name": "order_count",
                        "description": "Total orders",
                        "data_type": "INT64",
                        "tags": [],
                        "meta": {},
                    },
                },
                "tags": ["ml"],
                "meta": {},
                "depends_on": {"nodes": []},
            },
            "test.test_project.some_test": {
                "name": "some_test",
                "unique_id": "test.test_project.some_test",
                "resource_type": "test",
                "database": "my_database",
                "schema": "analytics",
                "alias": "some_test",
                "description": "A test node",
                "columns": {},
                "tags": [],
                "meta": {},
                "depends_on": {"nodes": []},
            },
        },
    }

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path


class TestDbtManifestParser:
    """Tests for DbtManifestParser class."""

    def test_parse_manifest(self, sample_manifest):
        """Test parsing a valid manifest file."""
        parser = DbtManifestParser(str(sample_manifest))
        parser.parse()

        assert parser.dbt_version == "1.5.0"
        assert parser.project_name == "test_project"

    def test_parse_manifest_not_found(self, tmp_path):
        """Test error when manifest file doesn't exist."""
        parser = DbtManifestParser(str(tmp_path / "nonexistent.json"))

        with pytest.raises(FileNotFoundError) as exc_info:
            parser.parse()

        assert "dbt manifest not found" in str(exc_info.value)

    def test_parse_manifest_invalid_json(self, tmp_path):
        """Test error when manifest is invalid JSON."""
        invalid_path = tmp_path / "invalid.json"
        invalid_path.write_text("not valid json {{{")

        parser = DbtManifestParser(str(invalid_path))

        with pytest.raises(ValueError) as exc_info:
            parser.parse()

        assert "Invalid JSON" in str(exc_info.value)

    def test_get_all_models(self, sample_manifest):
        """Test getting all models from manifest."""
        parser = DbtManifestParser(str(sample_manifest))
        models = parser.get_models()

        # Should only get models, not tests
        assert len(models) == 2
        model_names = [m.name for m in models]
        assert "driver_stats" in model_names
        assert "customer_stats" in model_names
        assert "some_test" not in model_names

    def test_get_models_by_name(self, sample_manifest):
        """Test filtering models by name."""
        parser = DbtManifestParser(str(sample_manifest))
        models = parser.get_models(model_names=["driver_stats"])

        assert len(models) == 1
        assert models[0].name == "driver_stats"

    def test_get_models_by_tag(self, sample_manifest):
        """Test filtering models by tag."""
        parser = DbtManifestParser(str(sample_manifest))

        # Filter by 'feast' tag - only driver_stats has it
        feast_models = parser.get_models(tag_filter="feast")
        assert len(feast_models) == 1
        assert feast_models[0].name == "driver_stats"

        # Filter by 'ml' tag - both models have it
        ml_models = parser.get_models(tag_filter="ml")
        assert len(ml_models) == 2

    def test_model_properties(self, sample_manifest):
        """Test DbtModel properties."""
        parser = DbtManifestParser(str(sample_manifest))
        model = parser.get_model_by_name("driver_stats")

        assert model is not None
        assert model.name == "driver_stats"
        assert model.unique_id == "model.test_project.driver_stats"
        assert model.database == "my_database"
        assert model.schema == "analytics"
        assert model.alias == "driver_stats"
        assert model.description == "Driver statistics aggregated hourly"
        assert model.full_table_name == "my_database.analytics.driver_stats"
        assert "feast" in model.tags
        assert "ml" in model.tags
        assert len(model.columns) == 4

    def test_column_properties(self, sample_manifest):
        """Test DbtColumn properties."""
        parser = DbtManifestParser(str(sample_manifest))
        model = parser.get_model_by_name("driver_stats")

        # Find the trip_count column
        trip_count_col = next(
            (c for c in model.columns if c.name == "trip_count"), None
        )

        assert trip_count_col is not None
        assert trip_count_col.name == "trip_count"
        assert trip_count_col.data_type == "INT64"
        assert trip_count_col.description == "Number of trips"
        assert "feature" in trip_count_col.tags

    def test_get_model_by_name_not_found(self, sample_manifest):
        """Test getting a model that doesn't exist."""
        parser = DbtManifestParser(str(sample_manifest))
        model = parser.get_model_by_name("nonexistent_model")

        assert model is None

    def test_depends_on(self, sample_manifest):
        """Test model dependencies are captured."""
        parser = DbtManifestParser(str(sample_manifest))
        model = parser.get_model_by_name("driver_stats")

        assert len(model.depends_on) == 1
        assert "source.test_project.raw_trips" in model.depends_on


class TestDbtColumn:
    """Tests for DbtColumn dataclass."""

    def test_column_defaults(self):
        """Test DbtColumn default values."""
        col = DbtColumn(name="test_col")

        assert col.name == "test_col"
        assert col.description == ""
        assert col.data_type == "STRING"
        assert col.tags == []
        assert col.meta == {}

    def test_column_with_all_fields(self):
        """Test DbtColumn with all fields specified."""
        col = DbtColumn(
            name="feature_col",
            description="A feature column",
            data_type="FLOAT64",
            tags=["feature", "numeric"],
            meta={"owner": "ml-team"},
        )

        assert col.name == "feature_col"
        assert col.description == "A feature column"
        assert col.data_type == "FLOAT64"
        assert col.tags == ["feature", "numeric"]
        assert col.meta == {"owner": "ml-team"}


class TestDbtModel:
    """Tests for DbtModel dataclass."""

    def test_model_full_table_name(self):
        """Test full_table_name property."""
        model = DbtModel(
            name="test_model",
            unique_id="model.proj.test_model",
            database="prod_db",
            schema="public",
            alias="test_model_v2",
        )

        assert model.full_table_name == "prod_db.public.test_model_v2"

    def test_model_defaults(self):
        """Test DbtModel default values."""
        model = DbtModel(
            name="test",
            unique_id="model.proj.test",
            database="db",
            schema="schema",
            alias="test",
        )

        assert model.description == ""
        assert model.columns == []
        assert model.tags == []
        assert model.meta == {}
        assert model.depends_on == []
