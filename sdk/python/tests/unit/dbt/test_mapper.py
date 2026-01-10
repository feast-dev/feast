"""
Unit tests for dbt to Feast mapper.
"""

import pytest
from datetime import timedelta

from feast.dbt.parser import DbtModel, DbtColumn
from feast.dbt.mapper import (
    DbtToFeastMapper,
    map_dbt_type_to_feast_type,
    DBT_TO_FEAST_TYPE_MAP,
)
from feast.types import (
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    UnixTimestamp,
    Bytes,
    Array,
)


class TestTypeMapping:
    """Tests for dbt to Feast type mapping."""

    def test_string_types(self):
        """Test string type mappings."""
        assert map_dbt_type_to_feast_type("STRING") == String
        assert map_dbt_type_to_feast_type("TEXT") == String
        assert map_dbt_type_to_feast_type("VARCHAR") == String
        assert map_dbt_type_to_feast_type("VARCHAR(255)") == String
        assert map_dbt_type_to_feast_type("CHAR") == String
        assert map_dbt_type_to_feast_type("NVARCHAR") == String

    def test_integer_types(self):
        """Test integer type mappings."""
        assert map_dbt_type_to_feast_type("INT") == Int64
        assert map_dbt_type_to_feast_type("INT64") == Int64
        assert map_dbt_type_to_feast_type("INTEGER") == Int64
        assert map_dbt_type_to_feast_type("BIGINT") == Int64
        assert map_dbt_type_to_feast_type("INT32") == Int32
        assert map_dbt_type_to_feast_type("SMALLINT") == Int32
        assert map_dbt_type_to_feast_type("TINYINT") == Int32

    def test_float_types(self):
        """Test float type mappings."""
        assert map_dbt_type_to_feast_type("FLOAT") == Float32
        assert map_dbt_type_to_feast_type("FLOAT32") == Float32
        assert map_dbt_type_to_feast_type("FLOAT64") == Float64
        assert map_dbt_type_to_feast_type("DOUBLE") == Float64
        assert map_dbt_type_to_feast_type("DOUBLE PRECISION") == Float64
        assert map_dbt_type_to_feast_type("REAL") == Float32

    def test_boolean_types(self):
        """Test boolean type mappings."""
        assert map_dbt_type_to_feast_type("BOOL") == Bool
        assert map_dbt_type_to_feast_type("BOOLEAN") == Bool

    def test_timestamp_types(self):
        """Test timestamp type mappings."""
        assert map_dbt_type_to_feast_type("TIMESTAMP") == UnixTimestamp
        assert map_dbt_type_to_feast_type("TIMESTAMP_NTZ") == UnixTimestamp
        assert map_dbt_type_to_feast_type("TIMESTAMP_LTZ") == UnixTimestamp
        assert map_dbt_type_to_feast_type("DATETIME") == UnixTimestamp
        assert map_dbt_type_to_feast_type("DATE") == UnixTimestamp

    def test_binary_types(self):
        """Test binary type mappings."""
        assert map_dbt_type_to_feast_type("BYTES") == Bytes
        assert map_dbt_type_to_feast_type("BINARY") == Bytes
        assert map_dbt_type_to_feast_type("VARBINARY") == Bytes
        assert map_dbt_type_to_feast_type("BLOB") == Bytes

    def test_case_insensitivity(self):
        """Test type mapping is case insensitive."""
        assert map_dbt_type_to_feast_type("string") == String
        assert map_dbt_type_to_feast_type("String") == String
        assert map_dbt_type_to_feast_type("STRING") == String
        assert map_dbt_type_to_feast_type("int64") == Int64
        assert map_dbt_type_to_feast_type("INT64") == Int64

    def test_parameterized_types(self):
        """Test parameterized types are handled correctly."""
        assert map_dbt_type_to_feast_type("VARCHAR(255)") == String
        assert map_dbt_type_to_feast_type("CHAR(10)") == String
        assert map_dbt_type_to_feast_type("DECIMAL(10,2)") == Int64

    def test_snowflake_number_precision(self):
        """Test Snowflake NUMBER type with precision."""
        # Small precision -> Int32
        assert map_dbt_type_to_feast_type("NUMBER(5,0)") == Int32
        assert map_dbt_type_to_feast_type("NUMBER(9,0)") == Int32

        # Medium precision -> Int64
        assert map_dbt_type_to_feast_type("NUMBER(10,0)") == Int64
        assert map_dbt_type_to_feast_type("NUMBER(18,0)") == Int64

        # Large precision -> Float64
        assert map_dbt_type_to_feast_type("NUMBER(20,0)") == Float64

        # With decimal places -> Float64
        assert map_dbt_type_to_feast_type("NUMBER(10,2)") == Float64
        assert map_dbt_type_to_feast_type("NUMBER(5,3)") == Float64

    def test_array_types(self):
        """Test ARRAY type mappings."""
        result = map_dbt_type_to_feast_type("ARRAY<STRING>")
        assert isinstance(result, Array)

        result = map_dbt_type_to_feast_type("ARRAY<INT64>")
        assert isinstance(result, Array)

        result = map_dbt_type_to_feast_type("ARRAY<FLOAT64>")
        assert isinstance(result, Array)

    def test_unknown_type_defaults_to_string(self):
        """Test unknown types default to String."""
        assert map_dbt_type_to_feast_type("UNKNOWN_TYPE") == String
        assert map_dbt_type_to_feast_type("CUSTOM") == String

    def test_empty_type_defaults_to_string(self):
        """Test empty type defaults to String."""
        assert map_dbt_type_to_feast_type("") == String
        assert map_dbt_type_to_feast_type(None) == String


@pytest.fixture
def sample_model():
    """Create a sample DbtModel for testing."""
    return DbtModel(
        name="driver_stats",
        unique_id="model.test_project.driver_stats",
        database="my_database",
        schema="analytics",
        alias="driver_stats",
        description="Driver statistics",
        columns=[
            DbtColumn(name="driver_id", data_type="INT64", description="Driver ID"),
            DbtColumn(
                name="event_timestamp",
                data_type="TIMESTAMP",
                description="Event time",
            ),
            DbtColumn(
                name="trip_count", data_type="INT64", description="Number of trips"
            ),
            DbtColumn(
                name="avg_rating", data_type="FLOAT64", description="Average rating"
            ),
            DbtColumn(
                name="is_active", data_type="BOOLEAN", description="Is driver active"
            ),
        ],
        tags=["feast", "ml"],
        meta={"owner": "ml-team"},
    )


class TestDbtToFeastMapper:
    """Tests for DbtToFeastMapper class."""

    def test_create_bigquery_data_source(self, sample_model):
        """Test creating a BigQuery data source."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        source = mapper.create_data_source(sample_model)

        assert source.name == "driver_stats_source"
        assert source.table == "my_database.analytics.driver_stats"
        assert source.timestamp_field == "event_timestamp"
        assert "dbt.model" in source.tags
        assert source.tags["dbt.model"] == "driver_stats"

    def test_create_snowflake_data_source(self, sample_model):
        """Test creating a Snowflake data source."""
        mapper = DbtToFeastMapper(data_source_type="snowflake")
        source = mapper.create_data_source(sample_model)

        assert source.name == "driver_stats_source"
        assert source.database == "my_database"
        assert source.schema == "analytics"
        assert source.table == "driver_stats"
        assert source.timestamp_field == "event_timestamp"

    def test_create_file_data_source(self, sample_model):
        """Test creating a file data source."""
        mapper = DbtToFeastMapper(data_source_type="file")
        source = mapper.create_data_source(sample_model)

        assert source.name == "driver_stats_source"
        assert "driver_stats.parquet" in source.path

    def test_unsupported_data_source_type(self, sample_model):
        """Test error for unsupported data source type."""
        mapper = DbtToFeastMapper(data_source_type="unsupported")

        with pytest.raises(ValueError) as exc_info:
            mapper.create_data_source(sample_model)

        assert "Unsupported data_source_type" in str(exc_info.value)

    def test_custom_timestamp_field(self, sample_model):
        """Test custom timestamp field."""
        mapper = DbtToFeastMapper(
            data_source_type="bigquery", timestamp_field="created_at"
        )
        source = mapper.create_data_source(sample_model)

        assert source.timestamp_field == "created_at"

    def test_create_entity(self):
        """Test creating an entity."""
        mapper = DbtToFeastMapper()
        entity = mapper.create_entity(
            name="driver_id",
            description="Driver entity",
            tags={"source": "dbt"},
        )

        assert entity.name == "driver_id"
        assert entity.join_key == "driver_id"
        assert entity.description == "Driver entity"
        assert entity.tags == {"source": "dbt"}

    def test_create_feature_view(self, sample_model):
        """Test creating a feature view."""
        mapper = DbtToFeastMapper(data_source_type="bigquery", ttl_days=7)
        source = mapper.create_data_source(sample_model)
        fv = mapper.create_feature_view(
            model=sample_model,
            source=source,
            entity_column="driver_id",
        )

        assert fv.name == "driver_stats"
        assert fv.ttl == timedelta(days=7)
        assert fv.description == "Driver statistics"

        # Check features (should exclude entity and timestamp)
        feature_names = [f.name for f in fv.features]
        assert "trip_count" in feature_names
        assert "avg_rating" in feature_names
        assert "is_active" in feature_names
        assert "driver_id" not in feature_names
        assert "event_timestamp" not in feature_names

        # Check tags
        assert "dbt.model" in fv.tags
        assert fv.tags["dbt.model"] == "driver_stats"
        assert "dbt.tag.feast" in fv.tags

    def test_create_feature_view_with_exclude(self, sample_model):
        """Test excluding columns from feature view."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        source = mapper.create_data_source(sample_model)
        fv = mapper.create_feature_view(
            model=sample_model,
            source=source,
            entity_column="driver_id",
            exclude_columns=["is_active"],
        )

        feature_names = [f.name for f in fv.features]
        assert "trip_count" in feature_names
        assert "avg_rating" in feature_names
        assert "is_active" not in feature_names

    def test_create_all_from_model(self, sample_model):
        """Test creating all Feast objects from a model."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        result = mapper.create_all_from_model(
            model=sample_model,
            entity_column="driver_id",
        )

        assert "entity" in result
        assert "data_source" in result
        assert "feature_view" in result

        assert result["entity"].name == "driver_id"
        assert result["data_source"].name == "driver_stats_source"
        assert result["feature_view"].name == "driver_stats"

    def test_feature_type_mapping(self, sample_model):
        """Test that feature types are correctly mapped."""
        mapper = DbtToFeastMapper(data_source_type="bigquery")
        source = mapper.create_data_source(sample_model)
        fv = mapper.create_feature_view(
            model=sample_model,
            source=source,
            entity_column="driver_id",
        )

        # Find specific features and check types
        trip_count = next((f for f in fv.features if f.name == "trip_count"), None)
        avg_rating = next((f for f in fv.features if f.name == "avg_rating"), None)
        is_active = next((f for f in fv.features if f.name == "is_active"), None)

        assert trip_count is not None
        assert trip_count.dtype == Int64

        assert avg_rating is not None
        assert avg_rating.dtype == Float64

        assert is_active is not None
        assert is_active.dtype == Bool
