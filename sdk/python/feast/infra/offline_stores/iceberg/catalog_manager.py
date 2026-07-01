"""Thin wrapper around ``pyiceberg`` for feature‑store operations.

All Iceberg REST Catalog interactions needed by Feast (table creation,
schema inspection, property management, and data writes) go through
this module so that the rest of the codebase never imports ``pyiceberg``
directly.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame as SparkDataFrame

from feast.infra.offline_stores.iceberg.catalog_config import IcebergCatalogConfig

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)

# Feast metadata stored as Iceberg table properties
FEAST_PRIMARY_KEYS_PROP = "feast.primary_keys"
FEAST_TIMESTAMP_KEY_PROP = "feast.timestamp_key"
FEAST_DESCRIPTION_PROP = "feast.description"
FEAST_OWNER_PROP = "feast.owner"
FEAST_PROJECT_PROP = "feast.project"
FEAST_MANAGED_PROP = "feast.managed"
_FEAST_TAG_PREFIX = "feast.tag."


def _build_catalog_props(config: IcebergCatalogConfig, token: str) -> Dict[str, str]:
    """Build the property dict passed to ``pyiceberg.catalog.load_catalog()``."""
    import os

    props: Dict[str, str] = {
        "uri": config.endpoint,
        "warehouse": config.warehouse,
    }
    token_val = token or os.environ.get(config.token_env_var, "")
    if token_val:
        props["token"] = token_val
    if config.credential_vending:
        props["credential-vending"] = "true"
    if config.rest_signing:
        props["rest-signing"] = "true"
    if config.warehouse_location:
        props["warehouse-location"] = config.warehouse_location
    return props


def load_catalog(
    config: IcebergCatalogConfig,
    token: str = "",
) -> "Catalog":
    """Create an Iceberg ``Catalog`` from Feast configuration."""
    from pyiceberg.catalog import load_catalog as _load_catalog

    props = _build_catalog_props(config, token)
    return _load_catalog(name=config.warehouse, type=config.type, **props)


def namespace_exists(catalog: "Catalog", namespace: str) -> bool:
    """Return ``True`` if the namespace (schema) exists in the catalog."""
    try:
        catalog.list_tables(namespace)
        return True
    except Exception:
        return False


def table_exists(catalog: "Catalog", namespace: str, table_name: str) -> bool:
    """Return ``True`` if the table exists in the catalog."""
    try:
        catalog.load_table((namespace, table_name))
        return True
    except Exception:
        return False


def _feast_to_iceberg_type(feast_dtype) -> str:
    """Convert a Feast ``dtype`` attribute to an Iceberg type string."""
    if feast_dtype is None:
        return "string"

    type_name = str(feast_dtype).upper()

    type_map = {
        "BOOL": "boolean",
        "BOOLEAN": "boolean",
        "INT8": "int",
        "BYTE": "int",
        "INT16": "int",
        "SHORT": "int",
        "INT32": "int",
        "INT": "int",
        "INTEGER": "int",
        "INT64": "long",
        "LONG": "long",
        "BIGINT": "long",
        "FLOAT32": "float",
        "FLOAT": "float",
        "FLOAT64": "double",
        "DOUBLE": "double",
        "STRING": "string",
        "UTF8": "string",
        "BINARY": "binary",
        "BYTES": "binary",
        "TIMESTAMP": "timestamp",
        "TIMESTAMP_TZ": "timestamptz",
        "UNIXTIMESTAMP": "timestamp",
        "DATE": "date",
        "DATE32": "date",
    }

    if type_name.startswith("LIST<") or type_name.startswith("ARRAY<"):
        return "list<string>"
    if type_name.startswith("STRUCT<") or type_name.startswith("MAP<"):
        return "string"

    return type_map.get(type_name, "string")


def _build_iceberg_schema_from_fields(
    entity_columns: Sequence,
    feature_columns: Sequence,
    timestamp_field: Optional[str] = None,
) -> "Schema":
    """Build a ``pyiceberg.schema.Schema`` from Feast column descriptors.

    Entity columns become non‑nullable; feature and timestamp columns
    are nullable.
    """
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
        TimestamptzType,
    )

    PYICEBERG_TYPE_MAP: Dict[str, Any] = {
        "boolean": BooleanType(),
        "int": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "string": StringType(),
        "binary": StringType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "date": DateType(),
    }

    fields: List[NestedField] = []
    seen: set = set()
    field_id = 1

    entity_types: Dict[str, str] = {}
    feat_types: Dict[str, str] = {}

    for col in entity_columns:
        t = _feast_to_iceberg_type(getattr(col, "dtype", None))
        entity_types[col.name] = t

    for col in feature_columns:
        t = _feast_to_iceberg_type(getattr(col, "dtype", None))
        feat_types[col.name] = t

    for col in entity_columns:
        if col.name not in seen:
            t = entity_types.get(col.name, "string")
            ice_type = PYICEBERG_TYPE_MAP.get(t, StringType())
            fields.append(NestedField(field_id, col.name, ice_type, required=True))
            seen.add(col.name)
            field_id += 1

    for col in feature_columns:
        if col.name not in seen:
            t = feat_types.get(col.name, "string")
            ice_type = PYICEBERG_TYPE_MAP.get(t, StringType())
            fields.append(NestedField(field_id, col.name, ice_type, required=False))
            seen.add(col.name)
            field_id += 1

    if timestamp_field and timestamp_field not in seen:
        fields.append(
            NestedField(field_id, timestamp_field, TimestampType(), required=False)
        )

    return Schema(*fields)


def _build_iceberg_properties(
    primary_keys: List[str],
    timestamp_key: Optional[str],
    description: str,
    owner: str,
    project: str,
    tags: Dict[str, str],
) -> Dict[str, str]:
    """Build Iceberg table properties from Feast metadata."""
    props: Dict[str, str] = {}

    if primary_keys:
        props[FEAST_PRIMARY_KEYS_PROP] = ",".join(primary_keys)
    if timestamp_key:
        props[FEAST_TIMESTAMP_KEY_PROP] = timestamp_key
    if description:
        props[FEAST_DESCRIPTION_PROP] = description
    if owner:
        props[FEAST_OWNER_PROP] = owner
    if project:
        props[FEAST_PROJECT_PROP] = project
    props[FEAST_MANAGED_PROP] = "true"

    for key, value in tags.items():
        safe_key = key.replace(".", "_")
        props[f"{_FEAST_TAG_PREFIX}{safe_key}"] = value

    return props


def create_feature_table(
    catalog: "Catalog",
    namespace: str,
    table_name: str,
    entity_columns: Sequence,
    feature_columns: Sequence,
    primary_keys: List[str],
    timestamp_field: Optional[str],
    description: str,
    owner: str,
    project: str,
    tags: Dict[str, str],
) -> None:
    """Create an Iceberg table in the catalog with Feast metadata.

    The table schema is derived from Feast entity + feature columns,
    and Feast metadata (primary keys, tags, etc.) is stored as Iceberg
    table properties.
    """
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    if not namespace_exists(catalog, namespace):
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

    schema = _build_iceberg_schema_from_fields(
        entity_columns, feature_columns, timestamp_field
    )
    properties = _build_iceberg_properties(
        primary_keys,
        timestamp_field,
        description,
        owner,
        project,
        tags,
    )

    catalog.create_table(
        identifier=(namespace, table_name),
        schema=schema,
        properties=properties,
    )


def update_table_properties(
    catalog: "Catalog",
    namespace: str,
    table_name: str,
    description: str,
    owner: str,
    project: str,
    tags: Dict[str, str],
) -> None:
    """Update Iceberg table properties with current Feast metadata."""
    table = catalog.load_table((namespace, table_name))

    new_props = _build_iceberg_properties(
        primary_keys=[],
        timestamp_key=None,
        description=description,
        owner=owner,
        project=project,
        tags=tags,
    )

    table.set_properties(**new_props)


def write_materialized_data(
    catalog: "Catalog",
    namespace: str,
    table_name: str,
    spark_df: SparkDataFrame,
    mode: str = "merge",
) -> None:
    """Write materialized feature data into an Iceberg table.

    Uses Spark DataFrame writer with Iceberg format for the actual
    write.  The catalog provides the table metadata; the write is
    performed via Spark's native Iceberg integration.

    Args:
        catalog: The Iceberg catalog.
        namespace: Target namespace (schema).
        table_name: Target table name.
        spark_df: Spark DataFrame containing the materialized data.
        mode: Write mode — ``"merge"``, ``"append"``, or
            ``"overwrite"``.  Defaults to ``"merge"``.
    """
    full_name = f"`{namespace}`.`{table_name}`"

    if mode == "merge":
        _merge_into(catalog, namespace, table_name, spark_df)
    elif mode == "append":
        spark_df.write.format("iceberg").mode("append").save(full_name)
    elif mode == "overwrite":
        spark_df.write.format("iceberg").mode("overwrite").save(full_name)
    else:
        raise ValueError(f"Unsupported write mode: {mode}")


def _merge_into(
    catalog: "Catalog",
    namespace: str,
    table_name: str,
    spark_df: SparkDataFrame,
) -> None:
    """Upsert data using Spark MERGE INTO (Delta/Iceberg SQL syntax).

    Requires Spark 3.x with Iceberg support.
    """
    full_name = f"{namespace}.{table_name}"
    temp_view = f"_feast_merge_{table_name}"

    spark_df.createOrReplaceTempView(temp_view)

    table = catalog.load_table((namespace, table_name))
    primary_keys_str = table.properties.get(FEAST_PRIMARY_KEYS_PROP, "")
    pk_cols = [pk.strip() for pk in primary_keys_str.split(",") if pk.strip()]

    if not pk_cols:
        spark_df.sql_ctx.sql(f"INSERT INTO {full_name} SELECT * FROM {temp_view}")
        return

    join_cond = " AND ".join(f"target.`{pk}` = source.`{pk}`" for pk in pk_cols)

    all_cols = [f.name for f in spark_df.schema.fields]
    update_set = ", ".join(f"target.`{c}` = source.`{c}`" for c in all_cols)
    insert_cols = ", ".join(f"`{c}`" for c in all_cols)
    source_cols = ", ".join(f"source.`{c}`" for c in all_cols)

    merge_sql = (
        f"MERGE INTO {full_name} AS target "
        f"USING {temp_view} AS source "
        f"ON {join_cond} "
        f"WHEN MATCHED THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({source_cols})"
    )

    spark_df.sql_ctx.sql(merge_sql)
