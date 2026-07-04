"""Iceberg REST Catalog registration for ``feast apply``.

When the offline store is configured with an Iceberg REST Catalog, this
module registers (or updates) each ``FeatureView`` as a table in the
catalog.  FeatureView entities become primary keys (stored as an Iceberg
table property), and tags/description/owner are synced to Iceberg table
properties.

Per‑FeatureView opt‑out and overrides are controlled via ``FeatureView``
``tags``:

* ``uc.register_as_feature_table`` — ``"false"`` skips a specific view.
* ``uc.catalog`` / ``uc.schema`` / ``uc.table`` — override the UC path.

Global defaults come from ``UCRegistrationConfig`` (inside
``DatabricksUCOfflineStoreConfig``).
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

import click

from feast import FeatureView
from feast.infra.offline_stores.iceberg.catalog_config import IcebergCatalogConfig
from feast.infra.offline_stores.iceberg.catalog_manager import (
    create_feature_table,
    load_catalog,
    update_table_properties,
    write_materialized_data,
)

logger = logging.getLogger(__name__)

# FeatureView tag keys for per‑FV configuration
_REGISTER_AS_FEATURE_TABLE_KEY = "uc.register_as_feature_table"
_CATALOG_KEY = "uc.catalog"
_SCHEMA_KEY = "uc.schema"
_TABLE_KEY = "uc.table"


def _should_register(fv: FeatureView) -> bool:
    """Return ``True`` unless the feature view opts out via tags."""
    return fv.tags.get(_REGISTER_AS_FEATURE_TABLE_KEY, "true").lower() != "false"


def _resolve_uc_path(
    fv: FeatureView,
    default_catalog: Optional[str],
    default_schema: Optional[str],
) -> Tuple[Optional[str], Optional[str], str]:
    """Resolve the (catalog, schema, table_name) for a feature view.

    Prioritises per‑FV tag overrides, then global defaults, then the
    feature view name as the table name.
    """
    catalog = fv.tags.get(_CATALOG_KEY) or default_catalog
    schema = fv.tags.get(_SCHEMA_KEY) or default_schema
    table = fv.tags.get(_TABLE_KEY) or fv.name

    table = table.replace("-", "_").replace(".", "_").replace(" ", "_")
    return catalog, schema, table


def _get_primary_keys(fv: FeatureView) -> List[str]:
    """Extract primary key column names from entity columns."""
    return [col.name for col in fv.entity_columns]


def _build_tags(fv: FeatureView, project: str) -> Dict[str, str]:
    """Build table tags, excluding internal ``uc.*`` keys."""
    tags: Dict[str, str] = {}
    for key, value in fv.tags.items():
        if not key.startswith("uc."):
            tags[key] = value
    tags["feast_managed"] = "feast"
    tags["feast_project"] = project
    return tags


def _register_single_feature_view(
    catalog,
    fv: FeatureView,
    project: str,
    default_catalog: Optional[str],
    default_schema: Optional[str],
) -> None:
    """Register or update a single FeatureView as a catalog table."""
    if not _should_register(fv):
        click.echo(f"  ⊘ Skipping catalog registration for {fv.name} (opt‑out)")
        return

    catalog_name, schema, table = _resolve_uc_path(fv, default_catalog, default_schema)
    if not catalog_name or not schema:
        click.echo(
            f"  ⚠ Cannot register {fv.name}: missing catalog or schema. "
            f"Set default_catalog/default_schema in feature_store.yaml or "
            f"use tags uc.catalog/uc.schema."
        )
        return

    namespace = schema
    table_name = table
    primary_keys = _get_primary_keys(fv)
    timestamp_field = getattr(fv.batch_source, "timestamp_field", None)
    description = fv.description or ""
    owner = fv.owner or ""
    tags = _build_tags(fv, project)

    try:
        create_feature_table(
            catalog=catalog,
            namespace=namespace,
            table_name=table_name,
            entity_columns=fv.entity_columns,
            feature_columns=fv.features,
            primary_keys=primary_keys,
            timestamp_field=timestamp_field,
            description=description,
            owner=owner,
            project=project,
            tags=tags,
        )
        click.echo(
            f"  ✓ Created catalog table: {catalog_name}.{namespace}.{table_name}"
        )
    except Exception as e:
        from pyiceberg.exceptions import TableAlreadyExistsError

        if not isinstance(e, TableAlreadyExistsError):
            raise

        update_table_properties(
            catalog=catalog,
            namespace=namespace,
            table_name=table_name,
            description=description,
            owner=owner,
            project=project,
            tags=tags,
        )
        click.echo(
            f"  ✓ Updated catalog table: {catalog_name}.{namespace}.{table_name}"
        )


def register_uc_feature_tables(
    catalog_config: IcebergCatalogConfig,
    feature_views: List[FeatureView],
    project: str,
) -> None:
    """Register or update FeatureViews as Iceberg REST Catalog tables.

    Skips silently when:
    - ``pyiceberg`` is not installed.
    - The catalog cannot be reached.
    - All feature views opt out.

    Per‑feature‑view errors are logged and do not halt the apply.
    """
    try:
        import pyiceberg  # noqa: F401
    except ImportError:
        logger.info(
            "pyiceberg is not installed; skipping catalog registration. "
            "Install with: pip install pyiceberg[pyarrow]"
        )
        return

    try:
        catalog = load_catalog(catalog_config)
    except Exception as e:
        logger.warning("Could not create Iceberg catalog for registration: %s", e)
        return

    for fv in feature_views:
        try:
            _register_single_feature_view(
                catalog,
                fv,
                project,
                catalog_config.warehouse,
                catalog_config.namespace,
            )
        except Exception:
            logger.exception(
                "Failed to register catalog table for FeatureView '%s'",
                fv.name,
            )
            click.echo(
                f"  ✗ Failed to register catalog table: {fv.name} "
                f"(check logs for details)"
            )


_MATERIALIZE_OFFLINE_KEY = "uc.materialize_offline"


def write_uc_materialized_data(
    catalog_config: IcebergCatalogConfig,
    fv: FeatureView,
    df: Any,
    project: str,
) -> None:
    """Write materialized features into an Iceberg REST Catalog table.

    Converts ``pa.Table`` to Spark DataFrame if needed, then performs
    a MERGE INTO via the catalog manager.

    Skips silently when any guard condition fails:
    - ``register_as_feature_table`` tag is ``"false"``
    - ``materialize_offline`` tag is ``"false"``
    - The UC path (catalog/schema) cannot be resolved
    - The catalog cannot be reached (logged)
    """
    if not _should_register(fv):
        logger.info("Skipping UC materialization for %s (opt-out)", fv.name)
        return

    if fv.tags.get(_MATERIALIZE_OFFLINE_KEY, "true").lower() == "false":
        logger.info(
            "Skipping UC materialization for %s (materialize_offline=false)",
            fv.name,
        )
        return

    catalog_name, schema, table = _resolve_uc_path(
        fv, catalog_config.warehouse, catalog_config.namespace
    )
    if not catalog_name or not schema:
        logger.warning(
            "Cannot materialize to UC for %s: missing catalog or schema.",
            fv.name,
        )
        return

    try:
        catalog = load_catalog(catalog_config)
    except Exception as e:
        logger.warning("Could not create Iceberg catalog for materialization: %s", e)
        return

    try:
        create_feature_table(
            catalog=catalog,
            namespace=schema,
            table_name=table,
            entity_columns=fv.entity_columns,
            feature_columns=fv.features,
            primary_keys=_get_primary_keys(fv),
            timestamp_field=getattr(fv.batch_source, "timestamp_field", None),
            description=fv.description or "",
            owner=fv.owner or "",
            project=project,
            tags=_build_tags(fv, project),
        )
        logger.info("Created catalog table %s.%s.%s", catalog_name, schema, table)
    except Exception as e:
        from pyiceberg.exceptions import TableAlreadyExistsError

        if not isinstance(e, TableAlreadyExistsError):
            logger.exception(
                "Failed to create catalog table %s.%s.%s",
                catalog_name,
                schema,
                table,
            )
            raise

    import pyarrow as pa

    if isinstance(df, pa.Table):
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            spark_df = spark.createDataFrame(df)
        except ImportError:
            logger.warning(
                "pyspark is not installed; skipping UC materialization for %s",
                fv.name,
            )
            return
    else:
        spark_df = df

    write_materialized_data(
        catalog=catalog,
        namespace=schema,
        table_name=table,
        spark_df=spark_df,
        mode="merge",
    )
    logger.info("Materialized features to %s.%s.%s", catalog_name, schema, table)
