# Iceberg source (contrib)

## Description

Iceberg data sources are tables managed by any supported Iceberg catalog. The `IcebergSource` class provides a unified interface with a configurable `catalog_type` parameter:

- **`"rest"`** (default): [Apache Iceberg REST Catalog specification](https://iceberg.apache.org/concepts/catalog/#decoupling-using-the-rest-catalog) — Unity Catalog, Apache Polaris, Nessie, Snowflake Open Catalog
- **`"hive"`**: Hive Metastore catalog
- **`"glue"`**: AWS Glue catalog
- **`"sql"`**: SQL-based (JDBC) catalog
- **`"dynamodb"`**: DynamoDB-based catalog

The data source carries catalog connection details (catalog_type, endpoint, warehouse, namespace, table, authentication). When the offline store (DuckDB, Spark) encounters this source, it resolves table metadata and credentials via the configured catalog at query time.

## Examples

### IcebergSource (REST catalog)

Works with any Iceberg REST Catalog:

```python
from feast.infra.data_sources.contrib.iceberg_catalog import IcebergSource

my_source = IcebergSource(
    catalog_type="rest",  # default
    endpoint="http://localhost:8081/api/2.1/unity-catalog/iceberg",
    warehouse="unity",
    namespace="default",
    table="driver_features",
    timestamp_field="event_timestamp",
    token_env_var="UC_TOKEN",
)
```

### IcebergSource (Hive Metastore)

```python
from feast.infra.data_sources.contrib.iceberg_catalog import IcebergSource

my_source = IcebergSource(
    catalog_type="hive",
    catalog_properties={"uri": "thrift://metastore:9083"},
    warehouse="my_warehouse",
    namespace="default",
    table="driver_features",
    timestamp_field="event_timestamp",
)
```

### IcebergSource (AWS Glue)

```python
from feast.infra.data_sources.contrib.iceberg_catalog import IcebergSource

my_source = IcebergSource(
    catalog_type="glue",
    catalog_properties={"region_name": "us-east-1"},
    warehouse="my_account",
    namespace="my_database",
    table="driver_features",
    timestamp_field="event_timestamp",
)
```

### UnityCatalogSource (with governance) {#unity-catalog-source}

Extends `IcebergSource` with Unity Catalog governance:

```python
from feast.infra.data_sources.contrib.iceberg_catalog import (
    UnityCatalogSource,
)

my_uc_source = UnityCatalogSource(
    warehouse="production",
    namespace="ml_features",
    table="driver_stats",
    timestamp_field="event_timestamp",
    register_as_feature_table=True,  # Register in UC on feast apply
    sync_lineage=True,               # Record lineage in UC
)
```

When `endpoint` is omitted, it defaults to `{DATABRICKS_HOST}/api/2.1/unity-catalog/iceberg`.
When `token_env_var` is omitted, it defaults to `DATABRICKS_TOKEN`.

### PolarisSource (Apache Polaris) {#polaris-source}

Extends `IcebergSource` for [Apache Polaris](https://polaris.apache.org/). `warehouse` is the Polaris
catalog name. When `endpoint` is omitted it defaults to `$POLARIS_HOST/api/catalog`.

```python
from feast.infra.data_sources.contrib.iceberg_catalog import PolarisSource

my_polaris_source = PolarisSource(
    warehouse="my_polaris_catalog",
    namespace="ml",
    table="driver_stats",
    timestamp_field="event_timestamp",
)
```

Authentication is read from the environment (only the variable *names* are stored in the registry):

1. OAuth2 client credentials: `POLARIS_CLIENT_ID` and `POLARIS_CLIENT_SECRET` (override the names with
   `client_id_env_var` / `client_secret_env_var`). Tokens are refreshed automatically before they expire.
2. Otherwise a bearer token from `POLARIS_TOKEN` (override with `token_env_var`).

To record Feast metadata on the Polaris tables when you run `feast apply`, select the Polaris provider:

```yaml
# feature_store.yaml
provider: polaris
```

For every feature view backed by a `PolarisSource` (with `sync_metadata=True`, the default) the provider sets
`feast.project`, `feast.feature_view`, `feast.owner`, `feast.description`, `feast.tag.<key>` and
`feast.is_feature_table` as table properties through the standard Iceberg REST `updateTable` operation.
When a feature view is deleted the table is kept and marked `feast.is_feature_table=false` with a
`feast.deleted_at` timestamp. A failed metadata sync is logged and never fails `feast apply`.

Polaris has no lineage API, so `sync_lineage` is not available.

To create a source for every table in a catalog:

```python
sources = PolarisSource.discover(
    warehouse="my_polaris_catalog",
    namespaces=["ml"],                      # omit to scan all namespaces
    timestamp_fields=("event_timestamp",),  # tables without such a column are skipped
)
```

### Full Feature View Example

```python
from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.types import Float64, Int64

from feast.infra.data_sources.contrib.iceberg_catalog import (
    UnityCatalogSource,
)

driver = Entity(name="driver_id", join_keys=["driver_id"])

driver_stats_source = UnityCatalogSource(
    warehouse="production",
    namespace="ml_features",
    table="driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    source=driver_stats_source,
    schema=[
        Field(name="conv_rate", dtype=Float64),
        Field(name="acc_rate", dtype=Float64),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    ttl=timedelta(days=1),
    online=True,
)
```

## Configuration Reference

### IcebergSource

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `catalog_type` | `str` | Catalog backend: `"rest"` (default), `"hive"`, `"glue"`, `"sql"`, `"dynamodb"` |
| `endpoint` | `str` | Catalog endpoint URL (required for `"rest"`, optional for others) |
| `warehouse` | `str` | Catalog/warehouse name |
| `namespace` | `str` | Schema/namespace within the catalog |
| `table` | `str` | Table name |
| `catalog_properties` | `dict` | Additional catalog-specific properties passed to PyIceberg |
| `timestamp_field` | `str` | Event timestamp column for point-in-time joins |
| `created_timestamp_column` | `str` | Optional column indicating row creation time |
| `token_env_var` | `str` | Environment variable name holding the auth token |
| `credential_vending` | `bool` | Whether to request scoped credentials (default: `True`) |
| `field_mapping` | `dict` | Column name mapping from source to feature names |

### UnityCatalogSource (additional parameters)

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `register_as_feature_table` | `bool` | Register as UC feature table on `feast apply` (default: `True`) |
| `sync_lineage` | `bool` | Sync lineage metadata to Unity Catalog (default: `True`) |

### PolarisSource (additional parameters)

`PolarisSource` always uses `catalog_type="rest"`; `warehouse` is the Polaris catalog name.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `endpoint` | `str` | Defaults to `$POLARIS_HOST/api/catalog` |
| `token_env_var` | `str` | Bearer token variable (default: `POLARIS_TOKEN`) |
| `client_id_env_var` | `str` | OAuth client id variable (default: `POLARIS_CLIENT_ID`) |
| `client_secret_env_var` | `str` | OAuth client secret variable (default: `POLARIS_CLIENT_SECRET`) |
| `oauth_scope` | `str` | OAuth scope (default: `PRINCIPAL_ROLE:ALL`) |
| `oauth_server_uri` | `str` | Token endpoint if not `<endpoint>/v1/oauth/tokens`, e.g. an external IdP |
| `credential_vending` | `bool` | Request vended storage credentials for reads/writes (default: `True`) |
| `sync_metadata` | `bool` | Sync Feast metadata to table properties on `feast apply` with the `polaris` provider (default: `True`) |

## Supported Types

| Iceberg Type | Feast Type |
| :--- | :--- |
| `boolean` | `BOOL` |
| `int` | `INT32` |
| `long` | `INT64` |
| `float` | `FLOAT` |
| `double` | `DOUBLE` |
| `string` | `STRING` |
| `binary` | `BYTES` |
| `timestamp` / `timestamptz` | `INT64` |
| `decimal` | `DOUBLE` |
| `uuid` | `STRING` |
