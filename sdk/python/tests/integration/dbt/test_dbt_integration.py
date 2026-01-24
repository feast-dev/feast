"""
Integration tests for dbt import functionality.

Tests the full dbt integration workflow including:
- Parsing dbt manifest.json files
- Creating Feast objects from dbt models
- Tag filtering and model selection
- Different data source types (bigquery, snowflake, file)
"""

import os
import tempfile
from pathlib import Path

import pytest

# Skip all tests if dbt-artifacts-parser is not installed
pytest.importorskip("dbt_artifacts_parser", reason="dbt-artifacts-parser not installed")

from feast.dbt.codegen import generate_feast_code  # noqa: E402
from feast.dbt.mapper import DbtToFeastMapper  # noqa: E402
from feast.dbt.parser import DbtManifestParser  # noqa: E402
from feast.entity import Entity  # noqa: E402
from feast.feature_view import FeatureView  # noqa: E402
from feast.infra.offline_stores.bigquery_source import BigQuerySource  # noqa: E402
from feast.infra.offline_stores.file_source import FileSource  # noqa: E402
from feast.infra.offline_stores.snowflake_source import SnowflakeSource  # noqa: E402
from feast.types import Float32, Float64, Int32, Int64, String  # noqa: E402

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
        entity = mapper.create_entity("driver_id", description="Driver entity")

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

        # Check that schema excludes entity and timestamp columns
        feature_names = {f.name for f in feature_view.schema}
        assert "driver_id" not in feature_names  # Entity column excluded
        assert "event_timestamp" not in feature_names  # Timestamp column excluded
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
        entity = mapper.create_entity("driver_id")

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
                entity = mapper.create_entity(entity_col)
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


class TestDbtMultiEntitySupport:
    """Test multiple entity support in dbt integration."""

    def test_create_feature_view_with_multiple_entities(self, parser):
        """Test creating FeatureView with multiple entity columns."""
        # Create a mock model with multiple entity columns
        from feast.dbt.parser import DbtColumn, DbtModel

        model = DbtModel(
            name="test_model",
            unique_id="model.test.test_model",
            database="test_db",
            schema="test_schema",
            alias="test_table",
            description="Test model",
            tags=[],
            columns=[
                DbtColumn(name="user_id", data_type="STRING", description=""),
                DbtColumn(name="merchant_id", data_type="STRING", description=""),
                DbtColumn(name="event_timestamp", data_type="TIMESTAMP", description=""),
                DbtColumn(name="amount", data_type="FLOAT64", description=""),
            ],
        )

        mapper = DbtToFeastMapper(data_source_type="bigquery")
        data_source = mapper.create_data_source(model)

        # Create with multiple entities
        feature_view = mapper.create_feature_view(
            model=model,
            source=data_source,
            entity_columns=["user_id", "merchant_id"],
        )

        assert len(feature_view.entities) == 2
        # Both entity columns should be excluded from features
        feature_names = {f.name for f in feature_view.schema}
        assert "user_id" not in feature_names
        assert "merchant_id" not in feature_names
        assert "amount" in feature_names

    def test_generate_code_with_multiple_entities(self):
        """Test generating code with multiple entity columns."""
        from feast.dbt.parser import DbtColumn, DbtModel

        model = DbtModel(
            name="transactions",
            unique_id="model.test.transactions",
            database="test_db",
            schema="test_schema",
            alias="transactions",
            description="Transaction features",
            tags=[],
            columns=[
                DbtColumn(name="user_id", data_type="STRING", description=""),
                DbtColumn(name="merchant_id", data_type="STRING", description=""),
                DbtColumn(name="event_timestamp", data_type="TIMESTAMP", description=""),
                DbtColumn(name="amount", data_type="FLOAT64", description=""),
            ],
        )

        code = generate_feast_code(
            models=[model],
            entity_columns=["user_id", "merchant_id"],  # Multiple
            data_source_type="bigquery",
            timestamp_field="event_timestamp",
            ttl_days=1,
            manifest_path="test/manifest.json",
            project_name="test",
        )

        # Verify multiple entity definitions
        assert code.count("Entity(") == 2
        assert 'name="user_id"' in code
        assert 'name="merchant_id"' in code

        # Verify feature view uses both entities
        assert "entities=[user_id, merchant_id]" in code

        # Code should be valid Python
        import ast

        ast.parse(code)

    def test_entity_column_validation(self):
        """Test validation of entity_columns parameter."""
        from feast.dbt.parser import DbtColumn, DbtModel

        model = DbtModel(
            name="test_model",
            unique_id="model.test.test_model",
            database="test_db",
            schema="test_schema",
            alias="test_table",
            description="",
            tags=[],
            columns=[
                DbtColumn(name="id", data_type="STRING", description=""),
                DbtColumn(name="event_timestamp", data_type="TIMESTAMP", description=""),
            ],
        )

        mapper = DbtToFeastMapper()
        data_source = mapper.create_data_source(model)

        # Empty entity_columns should raise error
        with pytest.raises(ValueError, match="At least one entity"):
            mapper.create_feature_view(
                model=model,
                source=data_source,
                entity_columns=[],
            )
