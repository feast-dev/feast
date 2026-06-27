"""Unity Catalog feature table registration for ``feast apply``.

When the offline store is configured as ``databricks_uc``, this module
registers (or updates) each FeatureView as a Unity Catalog feature table
via the Databricks ``FeatureEngineeringClient``.  FeatureView entities
become UC primary keys, and tags/description/owner are synced to UC metadata.

Per‑FeatureView opt‑out and overrides are controlled via FeatureView ``tags``:

* ``uc.register_as_feature_table`` — ``"false"`` skips a specific view.
* ``uc.catalog`` / ``uc.schema`` / ``uc.table`` — override the UC path.

Global defaults come from ``UCRegistrationConfig`` (inside
``DatabricksUCOfflineStoreConfig``).
"""

import logging
from typing import Dict, List, Optional, Tuple

import click
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
)

from feast import FeatureView
from feast.infra.offline_stores.contrib.spark_offline_store.databricks_uc import (
    DatabricksUCOfflineStoreConfig,
    get_databricks_session,
)

logger = logging.getLogger(__name__)

# FeatureView tag keys for per‑FV UC configuration
_REGISTER_AS_FEATURE_TABLE_KEY = "uc.register_as_feature_table"
_CATALOG_KEY = "uc.catalog"
_SCHEMA_KEY = "uc.schema"
_TABLE_KEY = "uc.table"

# Internal Feast tags stored on the UC table
_MANAGED_BY_TAG = "feast_managed"


def _feast_to_spark_type_simple(field):
    """Convert a Feast :class:`Field` to a pyspark :class:`DataType`.

    This is a best‑effort mapping; the underlying Spark‑read path performs
    full schema inference at query time.
    """
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        ByteType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        TimestampType,
    )

    dtype = getattr(field, "dtype", None)
    if dtype is None:
        return StringType()  # fallback

    type_name = str(dtype).upper()

    type_map = {
        "BOOL": BooleanType(),
        "BOOLEAN": BooleanType(),
        "INT8": ByteType(),
        "BYTE": ByteType(),
        "INT16": ShortType(),
        "SHORT": ShortType(),
        "INT32": IntegerType(),
        "INT": IntegerType(),
        "INTEGER": IntegerType(),
        "INT64": LongType(),
        "LONG": LongType(),
        "BIGINT": LongType(),
        "FLOAT32": FloatType(),
        "FLOAT": FloatType(),
        "FLOAT64": DoubleType(),
        "DOUBLE": DoubleType(),
        "STRING": StringType(),
        "UTF8": StringType(),
        "BINARY": BinaryType(),
        "TIMESTAMP": TimestampType(),
        "TIMESTAMP_TZ": TimestampType(),
        "DATE": DateType(),
        "DATE32": DateType(),
    }

    if type_name.startswith("LIST<") or type_name.startswith("ARRAY<"):
        return ArrayType(StringType())
    if "DOUBLE" in type_name or "FLOAT" in type_name and "64" in type_name:
        return DoubleType()
    if "LIST" in type_name or "ARRAY" in type_name:
        return ArrayType(StringType())

    return type_map.get(type_name, StringType())


def _should_register(fv: FeatureView) -> bool:
    """Return ``True`` unless the feature view opts out via tags."""
    return fv.tags.get(_REGISTER_AS_FEATURE_TABLE_KEY, "true").lower() != "false"


def _resolve_uc_path(
    fv: FeatureView,
    default_catalog: Optional[str],
    default_schema: Optional[str],
) -> Tuple[str, str, str]:
    """Resolve the (catalog, schema, table_name) for a feature view.

    Prioritises per‑FV tag overrides, then global defaults, then the
    feature view name as the table name.
    """
    catalog = fv.tags.get(_CATALOG_KEY) or default_catalog
    schema = fv.tags.get(_SCHEMA_KEY) or default_schema
    table = fv.tags.get(_TABLE_KEY) or fv.name

    # Sanitise: replace characters that are invalid in UC names
    table = table.replace("-", "_").replace(".", "_").replace(" ", "_")
    return catalog, schema, table


def _build_spark_schema(fv: FeatureView) -> StructType:
    """Build a pyspark ``StructType`` from the feature view's columns."""
    fields: List[StructField] = []

    seen: set = set()
    for col in fv.entity_columns:
        if col.name not in seen:
            fields.append(
                StructField(col.name, _feast_to_spark_type_simple(col), nullable=False)
            )
            seen.add(col.name)

    for col in fv.features:
        if col.name not in seen:
            fields.append(
                StructField(col.name, _feast_to_spark_type_simple(col), nullable=True)
            )
            seen.add(col.name)

    # Add timestamp column if the batch source declares one
    timestamp_field = getattr(fv.batch_source, "timestamp_field", None)
    if timestamp_field and timestamp_field not in seen:
        from pyspark.sql.types import TimestampType

        fields.append(StructField(timestamp_field, TimestampType(), nullable=True))

    return StructType(fields)


def _get_primary_keys(fv: FeatureView) -> List[str]:
    """Extract primary key column names from entity columns."""
    return [col.name for col in fv.entity_columns]


def _build_uc_tags(fv: FeatureView, project: str) -> Dict[str, str]:
    """Build UC table tags, excluding internal ``uc.*`` keys."""
    tags: Dict[str, str] = {}
    for key, value in fv.tags.items():
        if not key.startswith("uc."):
            tags[key] = value
    tags[_MANAGED_BY_TAG] = "feast"
    tags["feast_project"] = project
    return tags


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_full_table_name(catalog, schema, table):
    """Build a three-level table reference: ``catalog.schema.table``."""
    parts = []
    if catalog:
        parts.append(f"`{catalog}`")
    if schema:
        parts.append(f"`{schema}`")
    parts.append(f"`{table}`")
    return ".".join(parts)


def _table_exists(spark_session: SparkSession, full_name: str) -> bool:
    """Check whether a UC table exists."""
    try:
        return spark_session.catalog.tableExists(full_name)
    except Exception:
        return False


def _create_uc_feature_table(
    fe_client,
    spark_session: SparkSession,
    fv: FeatureView,
    full_name: str,
    primary_keys: List[str],
    project: str,
) -> None:
    """Create a new UC feature table via FeatureEngineeringClient."""
    spark_schema = _build_spark_schema(fv)
    timestamp_key = getattr(fv.batch_source, "timestamp_field", None)
    description = fv.description or ""
    tags = _build_uc_tags(fv, project)
    owner = fv.owner

    fe_client.create_table(
        name=full_name,
        primary_keys=primary_keys,
        schema=spark_schema,
        timestamp_key=timestamp_key,
        description=description,
        tags=tags,
    )

    if owner:
        try:
            spark_session.sql(
                f"ALTER TABLE {full_name} SET OWNER TO `{_escape_sql_string(owner)}`"
            )
        except Exception:
            logger.debug(
                "Could not set owner for UC table %s; continuing",
                full_name,
            )


def _update_uc_feature_table_metadata(
    spark_session: SparkSession,
    fv: FeatureView,
    full_name: str,
    project: str,
) -> None:
    """Update metadata on an existing UC feature table."""
    description = fv.description or ""
    owner = fv.owner
    tags = _build_uc_tags(fv, project)

    # Update description (COMMENT)
    try:
        spark_session.sql(
            f"COMMENT ON TABLE {full_name} IS '{_escape_sql_string(description)}'"
        )
    except Exception:
        logger.debug(
            "Could not update comment on UC table %s; continuing",
            full_name,
        )

    # Update tags
    if tags:
        tag_pairs = ", ".join(
            f"'{_escape_sql_string(k)}' = '{_escape_sql_string(v)}'"
            for k, v in tags.items()
        )
        try:
            spark_session.sql(f"ALTER TABLE {full_name} SET TAGS ({tag_pairs})")
        except Exception:
            logger.debug(
                "Could not update tags on UC table %s; continuing",
                full_name,
            )

    # Update owner
    if owner:
        try:
            spark_session.sql(
                f"ALTER TABLE {full_name} SET OWNER TO `{_escape_sql_string(owner)}`"
            )
        except Exception:
            logger.debug(
                "Could not update owner on UC table %s; continuing",
                full_name,
            )


def _register_single_feature_view(
    spark_session: SparkSession,
    fe_client,
    fv: FeatureView,
    project: str,
    default_catalog: Optional[str],
    default_schema: Optional[str],
) -> None:
    """Register or update a single FeatureView as a UC feature table."""
    if not _should_register(fv):
        click.echo(f"  ⊘ Skipping UC registration for {fv.name} (opt‑out)")
        return

    catalog, schema, table = _resolve_uc_path(fv, default_catalog, default_schema)
    if not catalog or not schema:
        click.echo(
            f"  ⚠ Cannot register {fv.name}: missing catalog or schema. "
            f"Set default_catalog/default_schema in feature_store.yaml or "
            f"use tags uc.catalog/uc.schema."
        )
        return

    full_name = _build_full_table_name(catalog, schema, table)
    primary_keys = _get_primary_keys(fv)

    if _table_exists(spark_session, full_name):
        _update_uc_feature_table_metadata(spark_session, fv, full_name, project)
        click.echo(f"  ✓ Updated UC feature table: {full_name}")
    else:
        _create_uc_feature_table(
            fe_client,
            spark_session,
            fv,
            full_name,
            primary_keys,
            project,
        )
        click.echo(f"  ✓ Created UC feature table: {full_name}")


def register_uc_feature_tables(
    config: DatabricksUCOfflineStoreConfig,
    feature_views: List[FeatureView],
    project: str,
) -> None:
    """Register or update FeatureViews as Unity Catalog feature tables.

    Skips silently when:
    - ``uc_registration`` is not configured or ``enabled`` is ``False``.
    - ``databricks-feature-engineering`` is not installed.
    - The Databricks Spark session cannot be created.

    Per‑feature‑view errors are logged and do not halt the apply.
    """
    uc_config = config.uc_registration
    if uc_config is None or not uc_config.enabled:
        return

    try:
        from databricks.feature_engineering import (
            FeatureEngineeringClient,  # noqa: F401
        )
    except ImportError:
        logger.info(
            "databricks-feature-engineering is not installed; "
            "skipping UC feature table registration. "
            "Install with: pip install databricks-feature-engineering"
        )
        return

    try:
        spark_session = get_databricks_session(config)
    except Exception as e:
        logger.warning(
            "Could not create Databricks Spark session for UC registration: %s", e
        )
        return

    fe_client = FeatureEngineeringClient()

    default_catalog = uc_config.catalog or config.default_catalog
    default_schema = uc_config.schema or config.default_schema

    for fv in feature_views:
        try:
            _register_single_feature_view(
                spark_session,
                fe_client,
                fv,
                project,
                default_catalog,
                default_schema,
            )
        except Exception:
            logger.exception(
                "Failed to register UC feature table for FeatureView '%s'",
                fv.name,
            )
            click.echo(
                f"  ✗ Failed to register UC feature table: {fv.name} "
                f"(check logs for details)"
            )
