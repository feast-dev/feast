"""
Integration tests for dbt import functionality.

Tests the full dbt integration workflow including:
- Parsing dbt manifest.json files
- Creating Feast objects from dbt models
- Tag filtering and model selection
- Different data source types (bigquery, snowflake, file)
"""

import ast
import os
import tempfile
from pathlib import Path

import pytest

# Skip all tests if dbt-artifacts-parser is not installed
pytest.importorskip("dbt_artifacts_parser", reason="dbt-artifacts-parser not installed")

from feast.dbt.codegen import (  # noqa: E402
    DbtCodeGenerator,
    _escape_description,
    _make_var_name,
    generate_feast_code,
)
from feast.dbt.mapper import DbtToFeastMapper  # noqa: E402
from feast.dbt.parser import DbtColumn, DbtManifestParser, DbtModel  # noqa: E402
from feast.entity import Entity  # noqa: E402
from feast.feature_view import FeatureView  # noqa: E402
from feast.infra.offline_stores.bigquery_source import BigQuerySource  # noqa: E402
from feast.infra.offline_stores.file_source import FileSource  # noqa: E402
from feast.infra.offline_stores.snowflake_source import SnowflakeSource  # noqa: E402
from feast.types import Float32, Float64, Int32, Int64, String  # noqa: E402
from feast.value_type import ValueType  # noqa: E402

# Get the path to the test dbt project
TEST_DBT_PROJECT_DIR = Path(__file__).parent / "test_dbt_project"
TEST_MANIFEST_PATH = TEST_DBT_PROJECT_DIR / "target" / "manifest.json"


@pytest.fixture
def manifest_path():
    """Path to the test dbt manifest.json file."""
    return str(TEST_MANIFEST_PATH)


@pytest.fixture
def parser(manifest_path):
    """Create a DbtManifestParser with the test manifest."""
    parser = DbtManifestParser(manifest_path)
    parser.parse()
    return parser


class TestDbtManifestParsing:
    """Test dbt manifest parsing functionality."""

    def test_parse_manifest_metadata(self, parser):
        """Test that manifest metadata is correctly parsed."""
        # dbt_version will vary based on installed version - just verify it's present
        assert parser.dbt_version is not None
        assert len(parser.dbt_version) > 0
        assert parser.project_name == "feast_integration_test"

    def test_get_all_models(self, parser):
        """Test retrieving all models from manifest."""
        models = parser.get_models()

        assert len(models) == 3
        model_names = {m.name for m in models}
        assert model_names == {
            "driver_features",
            "customer_features",
            "product_features",
        }

    def test_get_models_with_tag_filter(self, parser):
        """Test filtering models by dbt tag."""
        # Filter by 'ml' tag (driver_features and customer_features have it)
        ml_models = parser.get_models(tag_filter="ml")
        assert len(ml_models) == 2
        ml_names = {m.name for m in ml_models}
        assert ml_names == {"driver_features", "customer_features"}

        # Filter by 'recommendations' tag (only product_features has it)
        rec_models = parser.get_models(tag_filter="recommendations")
        assert len(rec_models) == 1
        assert rec_models[0].name == "product_features"

        # Filter by 'feast' tag (all models have it)
        feast_models = parser.get_models(tag_filter="feast")
        assert len(feast_models) == 3

    def test_get_models_by_name(self, parser):
        """Test filtering models by specific names."""
        models = parser.get_models(model_names=["driver_features", "customer_features"])

        assert len(models) == 2
        model_names = {m.name for m in models}
        assert model_names == {"driver_features", "customer_features"}

    def test_model_properties(self, parser):
        """Test that model properties are correctly extracted."""
        model = parser.get_model_by_name("driver_features")

        assert model is not None
        assert model.name == "driver_features"
        # Database and schema vary by adapter (e.g., DuckDB uses "memory"/"main")
        assert model.database is not None
        assert model.schema is not None
        assert model.alias == "driver_features"
        assert model.description == "Driver hourly features for ML models"
        # Verify full_table_name is built correctly from components
        assert model.full_table_name == f"{model.database}.{model.schema}.{model.alias}"
        assert "feast" in model.tags
        assert "ml" in model.tags
        assert len(model.columns) == 5

    def test_model_columns(self, parser):
        """Test that column metadata is correctly extracted."""
        model = parser.get_model_by_name("driver_features")

        column_dict = {col.name: col for col in model.columns}

        # Check entity column
        assert "driver_id" in column_dict
        driver_id_col = column_dict["driver_id"]
        assert driver_id_col.data_type == "int64"
        assert driver_id_col.description == "Unique driver identifier"

        # Check timestamp column
        assert "event_timestamp" in column_dict
        ts_col = column_dict["event_timestamp"]
        assert ts_col.data_type == "timestamp"

        # Check feature columns
        assert "conv_rate" in column_dict
        assert column_dict["conv_rate"].data_type == "float64"

        assert "avg_daily_trips" in column_dict
        assert column_dict["avg_daily_trips"].data_type == "int32"


class TestDbtToFeastMapping:
    """Test mapping dbt models to Feast objects."""

    def test_create_bigquery_data_source(self, parser):
        """Test creating BigQuery data source from dbt model."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("driver_features")

        data_source = mapper.create_data_source(model)

        assert isinstance(data_source, BigQuerySource)
        assert data_source.name == "driver_features_source"
        # Table name is built from model's full_table_name (database.schema.alias)
        assert data_source.table == model.full_table_name
        assert data_source.timestamp_field == "event_timestamp"
        assert data_source.description == model.description
        assert "dbt.model" in data_source.tags
        assert data_source.tags["dbt.model"] == "driver_features"

    def test_create_snowflake_data_source(self, parser):
        """Test creating Snowflake data source from dbt model."""
        mapper = DbtToFeastMapper(data_source_type="snowflake")
        model = parser.get_model_by_name("customer_features")

        data_source = mapper.create_data_source(model)

        assert isinstance(data_source, SnowflakeSource)
        assert data_source.name == "customer_features_source"
        # Database and schema come from the model (varies by adapter)
        assert data_source.database == model.database
        assert data_source.schema == model.schema
        assert data_source.table == "customer_features"
        assert data_source.timestamp_field == "event_timestamp"

    def test_create_file_data_source(self, parser):
        """Test creating File data source from dbt model."""
        mapper = DbtToFeastMapper(data_source_type="file")
        model = parser.get_model_by_name("product_features")

        data_source = mapper.create_data_source(model)

        assert isinstance(data_source, FileSource)
        assert data_source.name == "product_features_source"
        assert data_source.path == "/data/product_features.parquet"
        assert data_source.timestamp_field == "event_timestamp"

    def test_create_entity(self):
        """Test creating Feast Entity."""
        mapper = DbtToFeastMapper()

        entity = mapper.create_entity(
            name="driver_id",
            description="Driver entity",
        )

        assert isinstance(entity, Entity)
        assert entity.name == "driver_id"
        assert entity.join_key == "driver_id"
        assert entity.description == "Driver entity"

    def test_create_feature_view(self, parser):
        """Test creating Feast FeatureView from dbt model."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("driver_features")

        data_source = mapper.create_data_source(model)
        entity = mapper.create_entity(
            "driver_id", description="Driver entity", value_type=ValueType.INT64
        )

        feature_view = mapper.create_feature_view(
            model=model,
            source=data_source,
            entity_columns="driver_id",
            entities=entity,
            timestamp_field="event_timestamp",
            ttl_days=1,
        )

        assert isinstance(feature_view, FeatureView)
        assert feature_view.name == "driver_features"
        assert feature_view.source == data_source
        assert len(feature_view.entities) == 1
        assert feature_view.entities[0] == entity.name  # entities is a list of names
        assert feature_view.description == model.description

        # Check that schema excludes timestamp but includes entity columns
        # (FeatureView.__init__ expects entity columns in schema and extracts them)
        feature_names = {f.name for f in feature_view.schema}
        assert "event_timestamp" not in feature_names  # Timestamp column excluded
        assert "driver_id" in feature_names  # Entity column kept in schema
        assert "conv_rate" in feature_names
        assert "acc_rate" in feature_names
        assert "avg_daily_trips" in feature_names

        # Check feature types
        feature_dict = {f.name: f for f in feature_view.schema}
        assert feature_dict["conv_rate"].dtype == Float64
        assert feature_dict["acc_rate"].dtype == Float64
        assert feature_dict["avg_daily_trips"].dtype == Int32

    def test_create_feature_view_with_exclude_columns(self, parser):
        """Test creating FeatureView with excluded columns."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("driver_features")

        data_source = mapper.create_data_source(model)
        entity = mapper.create_entity("driver_id", value_type=ValueType.INT64)

        feature_view = mapper.create_feature_view(
            model=model,
            source=data_source,
            entity_columns="driver_id",
            entities=entity,
            exclude_columns=["acc_rate"],  # Exclude specific feature
        )

        feature_names = {f.name for f in feature_view.schema}
        assert "acc_rate" not in feature_names
        assert "conv_rate" in feature_names
        assert "avg_daily_trips" in feature_names

    def test_create_all_from_model(self, parser):
        """Test creating all Feast objects at once from dbt model."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("customer_features")

        objects = mapper.create_all_from_model(
            model=model,
            entity_columns="customer_id",
            ttl_days=2,
        )

        assert "entities" in objects
        assert "data_source" in objects
        assert "feature_view" in objects

        assert isinstance(objects["entities"], list)
        assert len(objects["entities"]) == 1
        assert isinstance(objects["entities"][0], Entity)
        assert objects["entities"][0].name == "customer_id"

        assert isinstance(objects["data_source"], BigQuerySource)
        assert isinstance(objects["feature_view"], FeatureView)

        # Verify the feature view uses the created entity and data source
        assert objects["feature_view"].source == objects["data_source"]
        assert objects["entities"][0].name in objects["feature_view"].entities


class TestDbtDataSourceTypes:
    """Test different data source type configurations."""

    @pytest.mark.parametrize(
        "data_source_type,expected_source_class",
        [
            ("bigquery", BigQuerySource),
            ("snowflake", SnowflakeSource),
            ("file", FileSource),
        ],
    )
    def test_all_data_source_types(
        self, parser, data_source_type, expected_source_class
    ):
        """Test creating data sources for all supported types."""
        mapper = DbtToFeastMapper(data_source_type=data_source_type)
        model = parser.get_model_by_name("driver_features")

        data_source = mapper.create_data_source(model)

        assert isinstance(data_source, expected_source_class)
        assert data_source.timestamp_field == "event_timestamp"

    def test_unsupported_data_source_type(self, parser):
        """Test that unsupported data source types raise an error."""
        mapper = DbtToFeastMapper(data_source_type="unsupported")
        model = parser.get_model_by_name("driver_features")

        with pytest.raises(ValueError, match="Unsupported data_source_type"):
            mapper.create_data_source(model)


class TestDbtCodeGeneration:
    """Test code generation functionality."""

    def test_generate_feast_code(self, parser):
        """Test generating Python code from dbt models."""
        models = parser.get_models(tag_filter="feast")

        code = generate_feast_code(
            models=models,
            entity_columns="driver_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="feast_integration_test",
            online=True,
        )

        # Verify generated code contains expected imports
        assert "from feast import Entity, FeatureView, Field" in code
        assert (
            "from feast.infra.offline_stores.bigquery_source import BigQuerySource"
            in code
        )

        # Verify entity definitions
        assert "Entity(" in code
        assert 'name="driver_id"' in code

        # Verify data source definitions
        assert "BigQuerySource(" in code
        assert "timestamp_field=" in code

        # Verify feature view definitions
        assert "FeatureView(" in code
        assert "schema=[" in code

    def test_generate_code_for_snowflake(self, parser):
        """Test generating code for Snowflake data sources."""
        models = parser.get_models(model_names=["customer_features"])

        code = generate_feast_code(
            models=models,
            entity_columns="customer_id",
            data_source_type="snowflake",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="feast_integration_test",
        )

        assert (
            "from feast.infra.offline_stores.snowflake_source import SnowflakeSource"
            in code
        )
        assert "SnowflakeSource(" in code

    def test_generate_code_for_file_source(self, parser):
        """Test generating code for File data sources."""
        models = parser.get_models(model_names=["product_features"])

        code = generate_feast_code(
            models=models,
            entity_columns="product_id",
            data_source_type="file",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="feast_integration_test",
        )

        assert "from feast.infra.offline_stores.file_source import FileSource" in code
        assert "FileSource(" in code


class TestDbtTypeMapping:
    """Test type mapping from dbt to Feast types."""

    def test_string_type_mapping(self, parser):
        """Test that string columns are mapped correctly."""
        model = parser.get_model_by_name("customer_features")
        mapper = DbtToFeastMapper()

        data_source = mapper.create_data_source(model)
        # Test that feature view creation works with string entity column
        fv = mapper.create_feature_view(
            model=model,
            source=data_source,
            entity_columns="customer_id",
        )

        # Verify feature view was created successfully
        # customer_id is a string type entity column
        assert fv is not None
        assert fv.name == "customer_features"

    def test_integer_type_mapping(self, parser):
        """Test that integer columns are mapped correctly."""
        model = parser.get_model_by_name("driver_features")
        mapper = DbtToFeastMapper()

        data_source = mapper.create_data_source(model)
        feature_view = mapper.create_feature_view(
            model=model,
            source=data_source,
            entity_columns="driver_id",
        )

        feature_dict = {f.name: f for f in feature_view.schema}

        # avg_daily_trips is INT32
        assert feature_dict["avg_daily_trips"].dtype == Int32

    def test_float_type_mapping(self, parser):
        """Test that float columns are mapped correctly."""
        model = parser.get_model_by_name("product_features")
        mapper = DbtToFeastMapper()

        data_source = mapper.create_data_source(model)
        feature_view = mapper.create_feature_view(
            model=model,
            source=data_source,
            entity_columns="product_id",
        )

        feature_dict = {f.name: f for f in feature_view.schema}

        # rating_avg is FLOAT32
        assert feature_dict["rating_avg"].dtype == Float32

        # view_count and purchase_count are INT64
        assert feature_dict["view_count"].dtype == Int64
        assert feature_dict["purchase_count"].dtype == Int64


class TestDbtIntegrationWorkflow:
    """Test the complete end-to-end dbt integration workflow."""

    def test_full_workflow_bigquery(self, parser):
        """Test complete workflow with BigQuery."""
        # Step 1: Parse manifest and filter models
        models = parser.get_models(tag_filter="feast")
        assert len(models) == 3

        # Step 2: Create mapper
        mapper = DbtToFeastMapper(
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=1,
        )

        # Step 3: Create Feast objects for each model
        all_objects = []
        entities = {}

        # Map entity columns to their value types based on the dbt schema
        entity_value_types = {
            "driver_id": ValueType.INT64,
            "customer_id": ValueType.STRING,
            "product_id": ValueType.STRING,
        }

        for model in models:
            # Determine entity column based on model
            if "driver" in model.name:
                entity_col = "driver_id"
            elif "customer" in model.name:
                entity_col = "customer_id"
            elif "product" in model.name:
                entity_col = "product_id"
            else:
                continue

            # Create or reuse entity
            if entity_col not in entities:
                entity = mapper.create_entity(
                    entity_col, value_type=entity_value_types[entity_col]
                )
                entities[entity_col] = entity
                all_objects.append(entity)
            else:
                entity = entities[entity_col]

            # Create data source
            data_source = mapper.create_data_source(model)
            all_objects.append(data_source)

            # Create feature view
            feature_view = mapper.create_feature_view(
                model=model,
                source=data_source,
                entity_columns=entity_col,
                entities=entity,
            )
            all_objects.append(feature_view)

        # Verify we created the right objects
        assert len(entities) == 3  # 3 unique entities
        assert len(all_objects) == 9  # 3 entities + 3 data sources + 3 feature views

    def test_code_generation_workflow(self, parser):
        """Test workflow that generates Python code."""
        models = parser.get_models(model_names=["driver_features"])

        # Generate code
        code = generate_feast_code(
            models=models,
            entity_columns="driver_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="feast_integration_test",
        )

        # Verify code is valid Python (basic check)
        assert code.startswith('"""')
        assert "from feast import" in code
        assert "Entity(" in code
        assert "FeatureView(" in code

        # Write to temp file and verify it can be read
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_path = f.name

        try:
            # Verify file was written
            assert os.path.exists(temp_path)
            with open(temp_path, "r") as f:
                read_code = f.read()
            assert read_code == code
        finally:
            os.unlink(temp_path)


class TestDbtErrorHandling:
    """Test error handling for edge cases."""

    def test_manifest_not_found(self):
        """Test that FileNotFoundError is raised for missing manifest."""
        parser = DbtManifestParser("/nonexistent/path/manifest.json")

        with pytest.raises(FileNotFoundError, match="dbt manifest not found"):
            parser.parse()

    def test_invalid_json_manifest(self, tmp_path):
        """Test that ValueError is raised for invalid JSON."""
        invalid_manifest = tmp_path / "manifest.json"
        invalid_manifest.write_text("{ invalid json }")

        parser = DbtManifestParser(str(invalid_manifest))

        with pytest.raises(ValueError, match="Invalid JSON"):
            parser.parse()


class TestDbtTypeMappingEdgeCases:
    """Test type mapping edge cases."""

    def test_unknown_type_falls_back_to_string(self):
        """Test that unknown types fall back to String."""
        from feast.dbt.mapper import map_dbt_type_to_feast_type

        result = map_dbt_type_to_feast_type("UNKNOWN_TYPE")
        assert result == String

    def test_empty_type_falls_back_to_string(self):
        """Test that empty/null types fall back to String."""
        from feast.dbt.mapper import map_dbt_type_to_feast_type

        assert map_dbt_type_to_feast_type("") == String
        # Test None handling (type: ignore for static analysis)
        assert map_dbt_type_to_feast_type(None) == String  # type: ignore[arg-type]

    def test_bool_type_mapping(self):
        """Test boolean type mapping."""
        from feast.dbt.mapper import map_dbt_type_to_feast_type
        from feast.types import Bool

        assert map_dbt_type_to_feast_type("BOOL") == Bool
        assert map_dbt_type_to_feast_type("BOOLEAN") == Bool
        assert map_dbt_type_to_feast_type("bool") == Bool

    def test_parameterized_varchar_mapping(self):
        """Test that VARCHAR(N) maps to String."""
        from feast.dbt.mapper import map_dbt_type_to_feast_type

        assert map_dbt_type_to_feast_type("VARCHAR(255)") == String
        assert map_dbt_type_to_feast_type("VARCHAR(100)") == String
        assert map_dbt_type_to_feast_type("CHAR(10)") == String

    def test_snowflake_number_with_precision(self):
        """Test Snowflake NUMBER with precision/scale mapping."""
        from feast.dbt.mapper import map_dbt_type_to_feast_type

        # No decimal places -> Int
        assert map_dbt_type_to_feast_type("NUMBER(10,0)") == Int64
        assert map_dbt_type_to_feast_type("NUMBER(5,0)") == Int32

        # With decimal places -> Float64
        assert map_dbt_type_to_feast_type("NUMBER(10,2)") == Float64
        assert map_dbt_type_to_feast_type("NUMBER(18,4)") == Float64

    def test_array_type_mapping(self):
        """Test ARRAY type mapping."""
        from feast.dbt.mapper import map_dbt_type_to_feast_type
        from feast.types import Array

        result = map_dbt_type_to_feast_type("ARRAY<STRING>")
        assert isinstance(result, Array)

        result = map_dbt_type_to_feast_type("ARRAY<INT64>")
        assert isinstance(result, Array)


class TestDbtDataSourceAdvanced:
    """Test advanced data source features."""

    def test_created_timestamp_column(self, parser):
        """Test that created_timestamp_column is passed correctly."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("driver_features")

        data_source = mapper.create_data_source(
            model,
            created_timestamp_column="created_at",
        )

        assert data_source.created_timestamp_column == "created_at"

    def test_custom_timestamp_field(self, parser):
        """Test custom timestamp field override."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("driver_features")

        data_source = mapper.create_data_source(
            model,
            timestamp_field="custom_ts",
        )

        assert data_source.timestamp_field == "custom_ts"


class TestDbtCodeGenerationAdvanced:
    """Test advanced code generation scenarios."""

    def test_generated_code_is_valid_python(self, parser):
        """Test that generated code can be parsed as valid Python."""
        import ast

        models = parser.get_models(model_names=["driver_features"])

        code = generate_feast_code(
            models=models,
            entity_columns="driver_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="test",
        )

        # This will raise SyntaxError if code is invalid
        ast.parse(code)

    def test_generated_code_has_correct_imports(self, parser):
        """Test that generated code has all necessary type imports."""
        models = parser.get_models(model_names=["driver_features"])

        code = generate_feast_code(
            models=models,
            entity_columns="driver_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="test",
        )

        # Check that all used types are imported
        assert "Float64" in code
        assert "Int32" in code


# ---------------------------------------------------------------------------
# Helper to build synthetic DbtModel objects for targeted tests
# ---------------------------------------------------------------------------
def _make_model(
    name="test_model",
    columns=None,
    tags=None,
    description="",
    database="testdb",
    schema="public",
):
    """Build a DbtModel without needing a manifest."""
    if columns is None:
        columns = [
            DbtColumn(name="entity_id", data_type="string"),
            DbtColumn(name="event_timestamp", data_type="timestamp"),
            DbtColumn(name="feature_a", data_type="float64"),
        ]
    return DbtModel(
        name=name,
        unique_id=f"model.test.{name}",
        database=database,
        schema=schema,
        alias=name,
        description=description,
        columns=columns,
        tags=tags or ["feast"],
    )


def _run_generated_code(code):
    """Write generated code to a temp module and import it, returning its namespace."""
    import importlib.util

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, prefix="feast_gen_"
    ) as f:
        f.write(code)
        mod_path = f.name

    try:
        spec = importlib.util.spec_from_file_location("_feast_gen", mod_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return vars(mod)
    finally:
        os.unlink(mod_path)


# ============================================================================
# Gap #1 – Generated code is never executed
# ============================================================================
class TestCodegenExecution:
    """Verify generated code actually produces valid Feast objects when run."""

    def test_run_generated_bigquery_code(self, parser):
        """Import generated BigQuery code and inspect the resulting objects."""
        models = parser.get_models(model_names=["customer_features"])
        code = generate_feast_code(
            models=models,
            entity_columns="customer_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=7,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="run_test",
        )

        ns = _run_generated_code(code)

        # Entity
        assert "customer_id" in ns
        entity = ns["customer_id"]
        assert isinstance(entity, Entity)
        assert entity.name == "customer_id"

        # DataSource
        assert "customer_features_source" in ns
        src = ns["customer_features_source"]
        assert isinstance(src, BigQuerySource)

        # FeatureView
        assert "customer_features_fv" in ns
        fv = ns["customer_features_fv"]
        assert isinstance(fv, FeatureView)
        assert fv.name == "customer_features"
        assert fv.online is True

    def test_run_generated_file_source_code(self, parser):
        """Import generated File source code."""
        models = parser.get_models(model_names=["product_features"])
        code = generate_feast_code(
            models=models,
            entity_columns="product_id",
            data_source_type="file",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="run_test",
        )
        ns = _run_generated_code(code)

        assert isinstance(ns["product_features_fv"], FeatureView)
        assert isinstance(ns["product_features_source"], FileSource)

    def test_run_generated_snowflake_code(self, parser):
        """Import generated Snowflake code."""
        models = parser.get_models(model_names=["customer_features"])
        code = generate_feast_code(
            models=models,
            entity_columns="customer_id",
            data_source_type="snowflake",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path=str(TEST_MANIFEST_PATH),
            project_name="run_test",
        )
        ns = _run_generated_code(code)

        assert isinstance(ns["customer_features_fv"], FeatureView)
        assert isinstance(ns["customer_features_source"], SnowflakeSource)


# ============================================================================
# Gap #2 – No multi-entity column tests
# ============================================================================
class TestMultiEntityColumns:
    """Test mapper and codegen with multiple entity columns."""

    @pytest.fixture
    def multi_entity_model(self):
        return _make_model(
            name="user_merchant_features",
            columns=[
                DbtColumn(name="user_id", data_type="string"),
                DbtColumn(name="merchant_id", data_type="string"),
                DbtColumn(name="event_timestamp", data_type="timestamp"),
                DbtColumn(name="txn_count", data_type="int64"),
                DbtColumn(name="total_amount", data_type="float64"),
            ],
            tags=["feast"],
            description="User-merchant interaction features",
        )

    def test_create_feature_view_multi_entity(self, multi_entity_model):
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        src = mapper.create_data_source(multi_entity_model)

        fv = mapper.create_feature_view(
            model=multi_entity_model,
            source=src,
            entity_columns=["user_id", "merchant_id"],
        )

        assert len(fv.entities) == 2
        entity_names = set(fv.entities)
        assert "user_id" in entity_names
        assert "merchant_id" in entity_names

    def test_create_all_from_model_multi_entity(self, multi_entity_model):
        mapper = DbtToFeastMapper(data_source_type="bigquery")

        result = mapper.create_all_from_model(
            model=multi_entity_model,
            entity_columns=["user_id", "merchant_id"],
        )

        assert len(result["entities"]) == 2
        assert result["entities"][0].name == "user_id"
        assert result["entities"][1].name == "merchant_id"

    def test_codegen_multi_entity(self, multi_entity_model):
        code = generate_feast_code(
            models=[multi_entity_model],
            entity_columns=["user_id", "merchant_id"],
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path="test",
            project_name="test",
        )

        # Verify both entities are in the generated code
        assert 'name="user_id"' in code
        assert 'name="merchant_id"' in code

        # Verify it's executable and produces correct objects
        ns = _run_generated_code(code)
        fv = ns["user_merchant_features_fv"]
        assert isinstance(fv, FeatureView)
        assert len(fv.entities) == 2

    def test_entity_columns_count_mismatch_raises(self, multi_entity_model):
        """Providing mismatched entity_columns and entities should raise."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        src = mapper.create_data_source(multi_entity_model)
        single_entity = mapper.create_entity("user_id")

        with pytest.raises(ValueError, match="must match"):
            mapper.create_feature_view(
                model=multi_entity_model,
                source=src,
                entity_columns=["user_id", "merchant_id"],
                entities=single_entity,  # 1 entity, 2 columns
            )


# ============================================================================
# Gap #3 – Codegen model-skipping is untested
# ============================================================================
class TestCodegenModelSkipping:
    """Verify codegen silently skips models missing required columns."""

    def test_skips_model_missing_timestamp(self):
        model = _make_model(
            name="no_timestamp",
            columns=[
                DbtColumn(name="entity_id", data_type="string"),
                DbtColumn(name="feature_a", data_type="float64"),
            ],
        )
        code = generate_feast_code(
            models=[model],
            entity_columns="entity_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
        )
        # Model should be skipped — no FeatureView generated
        assert "FeatureView(" not in code

    def test_skips_model_missing_entity_column(self):
        model = _make_model(
            name="no_entity",
            columns=[
                DbtColumn(name="event_timestamp", data_type="timestamp"),
                DbtColumn(name="feature_a", data_type="float64"),
            ],
        )
        code = generate_feast_code(
            models=[model],
            entity_columns="missing_col",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
        )
        assert "FeatureView(" not in code

    def test_partial_skip_with_multiple_models(self, parser):
        """When some models have required columns and some don't, only valid ones appear."""
        valid_model = parser.get_model_by_name("driver_features")
        invalid_model = _make_model(
            name="bad_model",
            columns=[DbtColumn(name="feature_a", data_type="float64")],
        )

        code = generate_feast_code(
            models=[valid_model, invalid_model],
            entity_columns="driver_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
        )

        assert "driver_features_fv" in code
        assert "bad_model" not in code


# ============================================================================
# Gap #4 – online=False never tested
# ============================================================================
class TestOnlineFalse:
    """Verify online=False propagates through mapper and codegen."""

    def test_feature_view_online_false(self, parser):
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("customer_features")
        src = mapper.create_data_source(model)

        fv = mapper.create_feature_view(
            model=model,
            source=src,
            entity_columns="customer_id",
            online=False,
        )
        assert fv.online is False

    def test_create_all_from_model_online_false(self, parser):
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("customer_features")

        result = mapper.create_all_from_model(
            model=model,
            entity_columns="customer_id",
            online=False,
        )
        assert result["feature_view"].online is False

    def test_codegen_online_false(self, parser):
        models = parser.get_models(model_names=["customer_features"])
        code = generate_feast_code(
            models=models,
            entity_columns="customer_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            online=False,
        )
        assert "online=False" in code

        ns = _run_generated_code(code)
        assert ns["customer_features_fv"].online is False


# ============================================================================
# Gap #5 – No CLI test coverage
# ============================================================================
class TestDbtCli:
    """Test the `feast dbt import` and `feast dbt list` CLI commands."""

    def test_dbt_list_command(self, manifest_path):
        from click.testing import CliRunner

        from feast.cli.dbt_import import list_command

        runner = CliRunner()
        result = runner.invoke(list_command, ["-m", manifest_path])
        assert result.exit_code == 0
        assert "driver_features" in result.output
        assert "customer_features" in result.output
        assert "product_features" in result.output

    def test_dbt_list_with_tag_filter(self, manifest_path):
        from click.testing import CliRunner

        from feast.cli.dbt_import import list_command

        runner = CliRunner()
        result = runner.invoke(list_command, ["-m", manifest_path, "--tag", "ml"])
        assert result.exit_code == 0
        assert "driver_features" in result.output
        assert "customer_features" in result.output
        # product_features doesn't have the 'ml' tag
        assert "product_features" not in result.output

    def test_dbt_list_show_columns(self, manifest_path):
        from click.testing import CliRunner

        from feast.cli.dbt_import import list_command

        runner = CliRunner()
        result = runner.invoke(list_command, ["-m", manifest_path, "--show-columns"])
        assert result.exit_code == 0
        assert "driver_id" in result.output
        assert "conv_rate" in result.output

    def test_dbt_import_dry_run(self, manifest_path):
        from click.testing import CliRunner

        from feast.cli.dbt_import import import_command

        runner = CliRunner()
        result = runner.invoke(
            import_command,
            [
                "-m",
                manifest_path,
                "-e",
                "driver_id",
                "-d",
                "bigquery",
                "--tag",
                "ml",
                "--dry-run",
            ],
            obj={"CHDIR": ".", "FS_YAML_FILE": "feature_store.yaml"},
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        assert "Dry run" in result.output

    def test_dbt_import_output_codegen(self, manifest_path, tmp_path):
        from click.testing import CliRunner

        from feast.cli.dbt_import import import_command

        output_file = str(tmp_path / "features.py")
        runner = CliRunner()
        result = runner.invoke(
            import_command,
            [
                "-m",
                manifest_path,
                "-e",
                "customer_id",
                "-d",
                "bigquery",
                "--tag",
                "ml",
                "--output",
                output_file,
            ],
            obj={"CHDIR": ".", "FS_YAML_FILE": "feature_store.yaml"},
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        assert os.path.exists(output_file)
        code = open(output_file).read()
        assert "FeatureView(" in code
        # Verify the generated file is valid python
        ast.parse(code)

    def test_dbt_import_duplicate_entity_columns(self, manifest_path):
        from click.testing import CliRunner

        from feast.cli.dbt_import import import_command

        runner = CliRunner()
        result = runner.invoke(
            import_command,
            [
                "-m",
                manifest_path,
                "-e",
                "driver_id",
                "-e",
                "driver_id",
                "-d",
                "bigquery",
                "--dry-run",
            ],
            obj={"CHDIR": ".", "FS_YAML_FILE": "feature_store.yaml"},
        )
        assert result.exit_code != 0
        assert "Duplicate" in result.output or "duplicate" in result.output.lower()

    def test_dbt_import_no_matching_models(self, manifest_path):
        from click.testing import CliRunner

        from feast.cli.dbt_import import import_command

        runner = CliRunner()
        result = runner.invoke(
            import_command,
            [
                "-m",
                manifest_path,
                "-e",
                "driver_id",
                "--tag",
                "nonexistent_tag",
                "--dry-run",
            ],
            obj={"CHDIR": ".", "FS_YAML_FILE": "feature_store.yaml"},
        )
        assert result.exit_code == 0
        assert "No models found" in result.output


# ============================================================================
# Gap #6 – Special characters in descriptions not tested through codegen
# ============================================================================
class TestCodegenSpecialCharacters:
    """Test that descriptions with special characters produce valid code."""

    @pytest.mark.parametrize(
        "desc",
        [
            'Model with "quoted" text',
            "Line1\nLine2",
            "Path\\to\\file",
            'Mixed "quotes" and\nnewlines\\here',
        ],
    )
    def test_description_escaping_produces_valid_code(self, desc):
        model = _make_model(name="special_desc_model", description=desc)
        code = generate_feast_code(
            models=[model],
            entity_columns="entity_id",
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
        )
        # Must be valid Python
        ast.parse(code)

        # Must be executable without errors
        ns = _run_generated_code(code)
        assert isinstance(ns["special_desc_model_fv"], FeatureView)


# ============================================================================
# Gap #7 – _make_var_name() with hyphens, spaces, digit-prefixed names
# ============================================================================
class TestMakeVarName:
    """Test variable name sanitization."""

    def test_hyphens_replaced(self):
        assert _make_var_name("my-model") == "my_model"

    def test_spaces_replaced(self):
        assert _make_var_name("my model") == "my_model"

    def test_digit_prefix_gets_underscore(self):
        assert _make_var_name("3phase") == "_3phase"

    def test_combined(self):
        assert _make_var_name("3-phase model") == "_3_phase_model"

    def test_already_valid(self):
        assert _make_var_name("good_name") == "good_name"


# ============================================================================
# Gap #8 – Validation error paths: empty entity_columns, entity count mismatch
# ============================================================================
class TestValidationErrors:
    """Test that invalid inputs produce clear errors."""

    def test_empty_entity_columns_in_feature_view(self, parser):
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        model = parser.get_model_by_name("driver_features")
        src = mapper.create_data_source(model)

        with pytest.raises(ValueError, match="At least one entity column"):
            mapper.create_feature_view(model=model, source=src, entity_columns=[])

    def test_empty_entity_columns_in_codegen(self):
        model = _make_model()
        with pytest.raises(ValueError, match="At least one entity column"):
            generate_feast_code(
                models=[model],
                entity_columns=[],
                data_source_type="bigquery",
                timestamp_field="event_timestamp",
            )


# ============================================================================
# Gap #9 – Combined filters (tag_filter + model_names) untested
# ============================================================================
class TestCombinedFilters:
    """Test parser with both tag_filter and model_names applied."""

    def test_tag_and_model_name_combined(self, parser):
        models = parser.get_models(
            tag_filter="ml",
            model_names=["driver_features"],
        )
        assert len(models) == 1
        assert models[0].name == "driver_features"

    def test_tag_and_model_name_no_overlap(self, parser):
        # product_features has 'recommendations' tag but not 'ml'
        models = parser.get_models(
            tag_filter="ml",
            model_names=["product_features"],
        )
        assert len(models) == 0

    def test_nonexistent_tag(self, parser):
        models = parser.get_models(tag_filter="nonexistent")
        assert len(models) == 0

    def test_nonexistent_model_name(self, parser):
        models = parser.get_models(model_names=["does_not_exist"])
        assert len(models) == 0


# ============================================================================
# Gap #10 – Lazy parse behavior untested
# ============================================================================
class TestLazyParse:
    """Verify get_models() auto-calls parse() if not called explicitly."""

    def test_get_models_without_explicit_parse(self, manifest_path):
        parser = DbtManifestParser(manifest_path)
        # Don't call parser.parse() — get_models should do it lazily
        models = parser.get_models()
        assert len(models) == 3

    def test_get_model_by_name_without_explicit_parse(self, manifest_path):
        parser = DbtManifestParser(manifest_path)
        model = parser.get_model_by_name("driver_features")
        assert model is not None
        assert model.name == "driver_features"


# ============================================================================
# Gap #11 – DbtCodeGenerator class never tested directly
# ============================================================================
class TestDbtCodeGeneratorClass:
    """Test the DbtCodeGenerator class directly (not the convenience function)."""

    def test_basic_generation(self, parser):
        gen = DbtCodeGenerator(
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=3,
        )
        models = parser.get_models(model_names=["driver_features"])
        code = gen.generate(
            models=models,
            entity_columns="driver_id",
            manifest_path="test/manifest.json",
            project_name="my_project",
        )
        ast.parse(code)
        assert "FeatureView(" in code
        assert "timedelta(days=3)" in code

    def test_exclude_columns(self, parser):
        gen = DbtCodeGenerator(data_source_type="bigquery")
        models = parser.get_models(model_names=["driver_features"])
        code = gen.generate(
            models=models,
            entity_columns="driver_id",
            exclude_columns=["acc_rate"],
        )
        assert "acc_rate" not in code
        assert "conv_rate" in code

    def test_case_insensitive_data_source_type(self, parser):
        gen = DbtCodeGenerator(data_source_type="BigQuery")
        models = parser.get_models(model_names=["driver_features"])
        code = gen.generate(models=models, entity_columns="driver_id")
        assert "BigQuerySource(" in code


# ============================================================================
# Additional: _escape_description edge cases
# ============================================================================
class TestEscapeDescription:
    """Test the _escape_description helper."""

    def test_none_returns_empty(self):
        assert _escape_description(None) == ""

    def test_empty_returns_empty(self):
        assert _escape_description("") == ""

    def test_quotes_escaped(self):
        assert '\\"' in _escape_description('has "quotes"')

    def test_newlines_replaced(self):
        result = _escape_description("line1\nline2")
        assert "\n" not in result

    def test_backslashes_escaped(self):
        result = _escape_description("path\\to\\file")
        assert "\\\\" in result
