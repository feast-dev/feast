"""
dbt to Feast type and object mapper.

This module provides functionality to map dbt model metadata to Feast objects
including DataSource, Entity, and FeatureView.
"""

from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

from feast.dbt.parser import DbtModel
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.types import (
    Array,
    Bool,
    Bytes,
    FeastType,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
)
from feast.value_type import ValueType

# Comprehensive mapping from dbt/warehouse types to Feast types
# Covers BigQuery, Snowflake, Redshift, PostgreSQL, and common SQL types
DBT_TO_FEAST_TYPE_MAP: Dict[str, FeastType] = {
    # String types
    "STRING": String,
    "TEXT": String,
    "VARCHAR": String,
    "CHAR": String,
    "CHARACTER": String,
    "NVARCHAR": String,
    "NCHAR": String,
    "CHARACTER VARYING": String,
    # Integer types
    "INT": Int64,
    "INT32": Int32,
    "INT64": Int64,
    "INTEGER": Int64,
    "BIGINT": Int64,
    "SMALLINT": Int32,
    "TINYINT": Int32,
    "BYTEINT": Int32,
    "NUMBER": Int64,  # Snowflake - default to Int64, precision handling below
    "NUMERIC": Int64,
    "DECIMAL": Int64,
    # Float types
    "FLOAT": Float32,
    "FLOAT32": Float32,
    "FLOAT64": Float64,
    "DOUBLE": Float64,
    "DOUBLE PRECISION": Float64,
    "REAL": Float32,
    # Boolean types
    "BOOL": Bool,
    "BOOLEAN": Bool,
    # Timestamp types
    "TIMESTAMP": UnixTimestamp,
    "TIMESTAMP_NTZ": UnixTimestamp,
    "TIMESTAMP_LTZ": UnixTimestamp,
    "TIMESTAMP_TZ": UnixTimestamp,
    "DATETIME": UnixTimestamp,
    "DATE": UnixTimestamp,
    "TIME": UnixTimestamp,
    # Binary types
    "BYTES": Bytes,
    "BINARY": Bytes,
    "VARBINARY": Bytes,
    "BLOB": Bytes,
}


def map_dbt_type_to_feast_type(dbt_type: str) -> FeastType:
    """
    Map a dbt data type to a Feast type.

    Handles various database type formats including:
    - Simple types: STRING, INT64, FLOAT
    - Parameterized types: VARCHAR(255), NUMBER(10,2), DECIMAL(18,0)
    - Array types: ARRAY<STRING>, ARRAY<INT64>

    Args:
        dbt_type: The dbt/database data type string

    Returns:
        The corresponding Feast type

    Examples:
        >>> map_dbt_type_to_feast_type("STRING")
        String
        >>> map_dbt_type_to_feast_type("INT64")
        Int64
        >>> map_dbt_type_to_feast_type("ARRAY<STRING>")
        Array(String)
    """
    if not dbt_type:
        return String

    # Normalize the type string
    normalized = dbt_type.upper().strip()

    # Handle ARRAY types: ARRAY<element_type>
    if normalized.startswith("ARRAY<") and normalized.endswith(">"):
        element_type_str = normalized[6:-1].strip()
        element_type = map_dbt_type_to_feast_type(element_type_str)
        # Array only supports primitive types
        valid_array_types = {
            String,
            Int32,
            Int64,
            Float32,
            Float64,
            Bool,
            Bytes,
            UnixTimestamp,
        }
        if element_type in valid_array_types:
            return Array(element_type)
        return Array(String)  # Fallback for complex nested types

    # Handle parameterized types: VARCHAR(255), NUMBER(10,2), etc.
    # Extract base type by removing parentheses and parameters
    base_type = normalized.split("(")[0].strip()

    # Handle Snowflake NUMBER with precision
    if base_type == "NUMBER" and "(" in normalized:
        try:
            # Parse precision and scale: NUMBER(precision, scale)
            params = normalized.split("(")[1].rstrip(")").split(",")
            precision = int(params[0].strip())
            scale = int(params[1].strip()) if len(params) > 1 else 0

            if scale > 0:
                # Has decimal places, use Float64
                return Float64
            elif precision <= 9:
                return Int32
            elif precision <= 18:
                return Int64
            else:
                # Precision > 18, may exceed Int64 range
                return Float64
        except (ValueError, IndexError):
            return Int64

    # Look up in mapping table
    if base_type in DBT_TO_FEAST_TYPE_MAP:
        return DBT_TO_FEAST_TYPE_MAP[base_type]

    # Default to String for unknown types
    return String


class DbtToFeastMapper:
    """
    Maps dbt models to Feast objects.

    Supports creating DataSource, Entity, and FeatureView objects from
    dbt model metadata.

    Examples:
        >>> mapper = DbtToFeastMapper(data_source_type="bigquery")
        >>> data_source = mapper.create_data_source(model)
        >>> feature_view = mapper.create_feature_view(
        ...     model, data_source, entity_column="driver_id"
        ... )

    Args:
        data_source_type: Type of data source ('bigquery', 'snowflake', 'file')
        timestamp_field: Default timestamp field name
        ttl_days: Default TTL in days for feature views
    """

    def __init__(
        self,
        data_source_type: str = "bigquery",
        timestamp_field: str = "event_timestamp",
        ttl_days: int = 1,
    ):
        self.data_source_type = data_source_type.lower()
        self.timestamp_field = timestamp_field
        self.ttl_days = ttl_days

    def create_data_source(
        self,
        model: DbtModel,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
    ) -> Any:
        """
        Create a Feast DataSource from a dbt model.

        Args:
            model: The DbtModel to create a DataSource from
            timestamp_field: Override the default timestamp field
            created_timestamp_column: Column for created timestamp (dedup)

        Returns:
            A Feast DataSource (BigQuerySource, SnowflakeSource, or FileSource)

        Raises:
            ValueError: If data_source_type is not supported
        """
        ts_field = timestamp_field or self.timestamp_field

        # Build tags from dbt metadata
        tags = {"dbt.model": model.name}
        for tag in model.tags:
            tags[f"dbt.tag.{tag}"] = "true"

        if self.data_source_type == "bigquery":
            from feast.infra.offline_stores.bigquery_source import BigQuerySource

            return BigQuerySource(
                name=f"{model.name}_source",
                table=model.full_table_name,
                timestamp_field=ts_field,
                created_timestamp_column=created_timestamp_column or "",
                description=model.description,
                tags=tags,
            )

        elif self.data_source_type == "snowflake":
            from feast.infra.offline_stores.snowflake_source import SnowflakeSource

            return SnowflakeSource(
                name=f"{model.name}_source",
                database=model.database,
                schema=model.schema,
                table=model.alias,
                timestamp_field=ts_field,
                created_timestamp_column=created_timestamp_column or "",
                description=model.description,
                tags=tags,
            )

        elif self.data_source_type == "file":
            from feast.infra.offline_stores.file_source import FileSource

            # For file sources, use the model name as a placeholder path
            return FileSource(
                name=f"{model.name}_source",
                path=f"/data/{model.name}.parquet",
                timestamp_field=ts_field,
                created_timestamp_column=created_timestamp_column or "",
                description=model.description,
                tags=tags,
            )

        else:
            raise ValueError(
                f"Unsupported data_source_type: {self.data_source_type}. "
                f"Supported types: bigquery, snowflake, file"
            )

    def create_entity(
        self,
        name: str,
        join_keys: Optional[List[str]] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        value_type: ValueType = ValueType.STRING,
    ) -> Entity:
        """
        Create a Feast Entity.

        Args:
            name: Entity name
            join_keys: List of join key column names (defaults to [name])
            description: Entity description
            tags: Optional tags
            value_type: Value type for the entity (default: STRING)

        Returns:
            A Feast Entity
        """
        return Entity(
            name=name,
            join_keys=join_keys or [name],
            value_type=value_type,
            description=description,
            tags=tags or {},
        )

    def create_feature_view(
        self,
        model: DbtModel,
        source: Any,
        entity_column: str,
        entity: Optional[Entity] = None,
        timestamp_field: Optional[str] = None,
        ttl_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        online: bool = True,
    ) -> FeatureView:
        """
        Create a Feast FeatureView from a dbt model.

        Args:
            model: The DbtModel to create a FeatureView from
            source: The DataSource for this FeatureView
            entity_column: The entity/primary key column name
            entity: Optional pre-created Entity (created if not provided)
            timestamp_field: Override the default timestamp field
            ttl_days: Override the default TTL in days
            exclude_columns: Additional columns to exclude from features
            online: Whether to enable online serving

        Returns:
            A Feast FeatureView
        """
        ts_field = timestamp_field or self.timestamp_field
        ttl = timedelta(days=ttl_days if ttl_days is not None else self.ttl_days)

        # Columns to exclude from features
        excluded = {entity_column, ts_field}
        if exclude_columns:
            excluded.update(exclude_columns)

        # Create schema from model columns
        schema: List[Field] = []
        for column in model.columns:
            if column.name not in excluded:
                feast_type = map_dbt_type_to_feast_type(column.data_type)
                schema.append(
                    Field(
                        name=column.name,
                        dtype=feast_type,
                        description=column.description,
                    )
                )

        # Create entity if not provided
        if entity is None:
            entity = self.create_entity(
                name=entity_column,
                description=f"Entity for {model.name}",
            )

        # Build tags from dbt metadata
        tags = {
            "dbt.model": model.name,
            "dbt.unique_id": model.unique_id,
        }
        for tag in model.tags:
            tags[f"dbt.tag.{tag}"] = "true"

        return FeatureView(
            name=model.name,
            source=source,
            schema=schema,
            entities=[entity],
            ttl=ttl,
            online=online,
            description=model.description,
            tags=tags,
        )

    def create_all_from_model(
        self,
        model: DbtModel,
        entity_column: str,
        timestamp_field: Optional[str] = None,
        ttl_days: Optional[int] = None,
        exclude_columns: Optional[List[str]] = None,
        online: bool = True,
    ) -> Dict[str, Union[Entity, Any, FeatureView]]:
        """
        Create all Feast objects (DataSource, Entity, FeatureView) from a dbt model.

        This is a convenience method that creates all necessary Feast objects
        in one call.

        Args:
            model: The DbtModel to create objects from
            entity_column: The entity/primary key column name
            timestamp_field: Override the default timestamp field
            ttl_days: Override the default TTL in days
            exclude_columns: Additional columns to exclude from features
            online: Whether to enable online serving

        Returns:
            Dict with keys 'entity', 'data_source', 'feature_view'
        """
        # Create entity
        entity = self.create_entity(
            name=entity_column,
            description=f"Entity for {model.name}",
            tags={"dbt.model": model.name},
        )

        # Create data source
        data_source = self.create_data_source(
            model=model,
            timestamp_field=timestamp_field,
        )

        # Create feature view
        feature_view = self.create_feature_view(
            model=model,
            source=data_source,
            entity_column=entity_column,
            entity=entity,
            timestamp_field=timestamp_field,
            ttl_days=ttl_days,
            exclude_columns=exclude_columns,
            online=online,
        )

        return {
            "entity": entity,
            "data_source": data_source,
            "feature_view": feature_view,
        }
