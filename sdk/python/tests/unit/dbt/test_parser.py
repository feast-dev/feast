"""
Unit tests for dbt manifest parser.
"""

import json

import pytest

# Skip all tests in this module if dbt-artifacts-parser is not installed
pytest.importorskip("dbt_artifacts_parser", reason="dbt-artifacts-parser not installed")

from feast.dbt.parser import DbtColumn, DbtManifestParser, DbtModel


def _create_model_node(
    name: str,
    unique_id: str,
    database: str = "my_database",
    schema: str = "analytics",
    description: str = "",
    columns: dict = None,
    tags: list = None,
    meta: dict = None,
    depends_on_nodes: list = None,
):
    """Helper to create a complete model node for dbt-artifacts-parser."""
    return {
        "name": name,
        "unique_id": unique_id,
        "resource_type": "model",
        "package_name": "test_project",
        "path": f"models/{name}.sql",
        "original_file_path": f"models/{name}.sql",
        "fqn": ["test_project", name],
        "alias": name,
        "checksum": {"name": "sha256", "checksum": "abc123"},
        "database": database,
        "schema": schema,
        "description": description or "",
        "columns": columns or {},
        "tags": tags or [],
        "meta": meta or {},
        "config": {
            "enabled": True,
            "materialized": "table",
            "tags": tags or [],
            "meta": meta or {},
        },
        "depends_on": {"nodes": depends_on_nodes or [], "macros": []},
        "refs": [],
        "sources": [],
        "metrics": [],
        "compiled_path": f"target/compiled/test_project/models/{name}.sql",
    }


def _create_column(
    name: str,
    data_type: str = "STRING",
    description: str = "",
    tags: list = None,
    meta: dict = None,
):
    """Helper to create a column definition."""
    return {
        "name": name,
        "description": description or "",
        "data_type": data_type,
        "tags": tags or [],
        "meta": meta or {},
    }


@pytest.fixture
def sample_manifest(tmp_path):
    """Create a sample dbt manifest.json for testing."""
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9.json",
            "dbt_version": "1.5.0",
            "generated_at": "2024-01-10T00:00:00Z",
            "invocation_id": "test-invocation-id",
            "env": {},
            "adapter_type": "bigquery",
        },
        "nodes": {
            "model.test_project.driver_stats": _create_model_node(
                name="driver_stats",
                unique_id="model.test_project.driver_stats",
                description="Driver statistics aggregated hourly",
                columns={
                    "driver_id": _create_column(
                        "driver_id", "INT64", "Unique driver identifier", ["entity"]
                    ),
                    "event_timestamp": _create_column(
                        "event_timestamp", "TIMESTAMP", "Event timestamp"
                    ),
                    "trip_count": _create_column(
                        "trip_count", "INT64", "Number of trips", ["feature"]
                    ),
                    "avg_rating": _create_column(
                        "avg_rating", "FLOAT64", "Average driver rating", ["feature"]
                    ),
                },
                tags=["feast", "ml"],
                meta={"owner": "data-team"},
                depends_on_nodes=["source.test_project.raw_trips"],
            ),
            "model.test_project.customer_stats": _create_model_node(
                name="customer_stats",
                unique_id="model.test_project.customer_stats",
                description="Customer statistics",
                columns={
                    "customer_id": _create_column(
                        "customer_id", "STRING", "Unique customer ID"
                    ),
                    "event_timestamp": _create_column(
                        "event_timestamp", "TIMESTAMP", "Event timestamp"
                    ),
                    "order_count": _create_column(
                        "order_count", "INT64", "Total orders"
                    ),
                },
                tags=["ml"],
            ),
        },
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "parent_map": {},
        "child_map": {},
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

        assert len(models) == 2
        model_names = [m.name for m in models]
        assert "driver_stats" in model_names
        assert "customer_stats" in model_names

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
