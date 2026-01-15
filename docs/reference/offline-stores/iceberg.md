# Iceberg offline store

## Description

The Iceberg offline store provides native support for [Apache Iceberg](https://iceberg.apache.org/) tables using [PyIceberg](https://py.iceberg.apache.org/). It offers a modern, open table format with ACID transactions, schema evolution, and time travel capabilities for feature engineering at scale.

**Key Features:**
* Native Iceberg table format support via PyIceberg
* Hybrid read strategy: Copy-on-Write (COW) and Merge-on-Read (MOR) optimization
* Point-in-time correct joins using DuckDB SQL engine
* Support for multiple catalog types (REST, Glue, Hive, SQL)
* Schema evolution and versioning
* Efficient metadata pruning for large tables
* Compatible with data lakes (S3, GCS, Azure Blob Storage)

**Read Strategy:**
* **COW Tables** (no deletes): Direct Parquet reading via DuckDB for maximum performance
* **MOR Tables** (with deletes): In-memory Arrow table loading for data correctness

Entity dataframes can be provided as a Pandas dataframe or SQL query.

## Getting started

In order to use this offline store, you'll need to install the Iceberg dependencies:

```bash
uv sync --extra iceberg
```

Or if using pip:
```bash
pip install 'feast[iceberg]'
```

This installs:
* `pyiceberg[sql,duckdb]>=0.8.0` - Native Iceberg table operations
* `duckdb>=1.0.0` - SQL engine for point-in-time joins

## Example

### Basic Configuration (REST Catalog)

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: iceberg
    catalog_type: rest
    catalog_name: feast_catalog
    uri: http://localhost:8181
    warehouse: s3://my-bucket/warehouse
    namespace: feast
online_store:
    type: sqlite
    path: data/online_store.db
```
{% endcode %}

### AWS Glue Catalog Configuration

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: iceberg
    catalog_type: glue
    catalog_name: feast_catalog
    warehouse: s3://my-bucket/warehouse
    namespace: feast
    storage_options:
        s3.region: us-west-2
        s3.access-key-id: ${AWS_ACCESS_KEY_ID}
        s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
online_store:
    type: dynamodb
    region: us-west-2
```
{% endcode %}

### Local Development (SQL Catalog)

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: iceberg
    catalog_type: sql
    catalog_name: feast_catalog
    uri: sqlite:///data/iceberg_catalog.db
    warehouse: data/warehouse
    namespace: feast
online_store:
    type: sqlite
    path: data/online_store.db
```
{% endcode %}

## Configuration Options

The full set of configuration options is available in `IcebergOfflineStoreConfig`:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | str | Yes | - | Must be `iceberg` |
| `catalog_type` | str | Yes | `"rest"` | Type of Iceberg catalog: `rest`, `glue`, `hive`, `sql` |
| `catalog_name` | str | Yes | `"feast_catalog"` | Name of the Iceberg catalog |
| `uri` | str | No | - | Catalog URI (required for REST/SQL catalogs) |
| `warehouse` | str | Yes | `"warehouse"` | Warehouse path (S3/GCS/local path) |
| `namespace` | str | No | `"feast"` | Iceberg namespace for feature tables |
| `storage_options` | dict | No | `{}` | Additional storage configuration (e.g., S3 credentials) |

## Data Source Configuration

To use Iceberg tables as feature sources:

```python
from feast import Field
from feast.types import Int64, String, Float32
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)

# Define an Iceberg data source
my_iceberg_source = IcebergSource(
    name="driver_stats",
    table_identifier="feast.driver_hourly_stats",  # namespace.table_name
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Use in a Feature View
driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    source=my_iceberg_source,
    ttl=timedelta(days=1),
)
```

## Functionality Matrix

The set of functionality supported by offline stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Iceberg offline store.

|                                                                    | Iceberg |
| :----------------------------------------------------------------- | :-----  |
| `get_historical_features` (point-in-time correct join)             | yes     |
| `pull_latest_from_table_or_query` (retrieve latest feature values) | yes     |
| `pull_all_from_table_or_query` (retrieve a saved dataset)          | yes     |
| `offline_write_batch` (persist dataframes to offline store)        | yes     |
| `write_logged_features` (persist logged features to offline store) | yes     |

Below is a matrix indicating which functionality is supported by `IcebergRetrievalJob`.

|                                                       | Iceberg |
| ----------------------------------------------------- | -----   |
| export to dataframe                                   | yes     |
| export to arrow table                                 | yes     |
| export to arrow batches                               | no      |
| export to SQL                                         | no      |
| export to data lake (S3, GCS, etc.)                   | no      |
| export to data warehouse                              | no      |
| export as Spark dataframe                             | no      |
| local execution of Python-based on-demand transforms  | yes     |
| remote execution of Python-based on-demand transforms | no      |
| persist results in the offline store                  | yes     |
| preview the query plan before execution               | no      |
| read partitioned data                                 | yes     |

To compare this set of functionality against other offline stores, please see the full [functionality matrix](overview.md#functionality-matrix).

## Performance Considerations

### Read Optimization

The Iceberg offline store automatically selects the optimal read strategy:

* **COW Tables**: Direct Parquet file reading via DuckDB for maximum performance
* **MOR Tables**: In-memory Arrow table loading to handle delete files correctly

This hybrid approach balances performance and correctness based on table characteristics.

### Metadata Pruning

Iceberg's metadata layer enables efficient partition pruning and file skipping:

```python
# Iceberg automatically prunes partitions and data files based on filters
# No full table scan required for filtered queries
historical_features = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
    ],
)
```

### Best Practices

1. **Partition Strategy**: Use appropriate partitioning (by date, entity, etc.) for your access patterns
2. **Compaction**: Periodically compact small files to maintain read performance
3. **Catalog Selection**: Use REST catalog for production, SQL catalog for local development
4. **Storage Credentials**: Store sensitive credentials in environment variables, not in YAML

## Catalog Types

### REST Catalog (Recommended for Production)

```yaml
offline_store:
    type: iceberg
    catalog_type: rest
    catalog_name: feast_catalog
    uri: http://iceberg-rest:8181
    warehouse: s3://data-lake/warehouse
```

### AWS Glue Catalog

```yaml
offline_store:
    type: iceberg
    catalog_type: glue
    catalog_name: feast_catalog
    warehouse: s3://data-lake/warehouse
    storage_options:
        s3.region: us-west-2
```

### Hive Metastore

```yaml
offline_store:
    type: iceberg
    catalog_type: hive
    catalog_name: feast_catalog
    uri: thrift://hive-metastore:9083
    warehouse: s3://data-lake/warehouse
```

### SQL Catalog (Local Development)

```yaml
offline_store:
    type: iceberg
    catalog_type: sql
    catalog_name: feast_catalog
    uri: sqlite:///data/iceberg_catalog.db
    warehouse: data/warehouse
```

## Cloudflare R2 Configuration

Cloudflare R2 provides S3-compatible object storage that works seamlessly with Iceberg. Here's how to configure Feast with R2:

### Using R2 with S3-Compatible Storage

```yaml
offline_store:
    type: iceberg
    catalog_type: sql  # or rest, hive, glue
    catalog_name: r2_catalog
    uri: postgresql://user:pass@catalog-host:5432/iceberg  # Catalog database
    warehouse: s3://my-r2-bucket/warehouse
    storage_options:
        s3.endpoint: https://<account-id>.r2.cloudflarestorage.com
        s3.access-key-id: ${R2_ACCESS_KEY_ID}
        s3.secret-access-key: ${R2_SECRET_ACCESS_KEY}
        s3.region: auto
        s3.force-virtual-addressing: true  # Required for R2
```

**Important R2 Configuration Notes:**
- `s3.force-virtual-addressing: true` is **required** for R2 compatibility
- Use `s3.region: auto` (R2 doesn't require specific regions)
- R2 endpoint format: `https://<account-id>.r2.cloudflarestorage.com`
- Get credentials from R2 dashboard: API Tokens → Create API Token

### Using R2 Data Catalog (Beta)

Cloudflare R2 also supports native Iceberg REST catalogs:

```yaml
offline_store:
    type: iceberg
    catalog_type: rest
    catalog_name: r2_data_catalog
    uri: <r2-catalog-uri>  # From R2 Data Catalog dashboard
    warehouse: <r2-warehouse-name>
    storage_options:
        token: ${R2_DATA_CATALOG_TOKEN}
```

**Benefits of R2:**
- **Cost-effective**: No egress fees, predictable pricing
- **Performance**: Global edge network for fast data access
- **S3-compatible**: Works with existing Iceberg/PyIceberg tooling
- **Integrated catalog**: Optional native Iceberg catalog support

**Resources:**
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/)
- [R2 S3 API Compatibility](https://developers.cloudflare.com/r2/api/s3/)
- [PyIceberg S3 Configuration](https://py.iceberg.apache.org/configuration/#s3)

## Schema Evolution

Iceberg supports schema evolution natively. When feature schemas change, Iceberg handles:
* Adding new columns
* Removing columns
* Renaming columns
* Type promotions (e.g., int32 to int64)

Changes are tracked in the metadata layer without rewriting data files.

## Time Travel

Leverage Iceberg's time travel capabilities for reproducible feature engineering:

```python
# Access historical table snapshots for point-in-time correct features
# Iceberg automatically uses the correct snapshot based on timestamps
```

## Limitations

* **Write Path**: Currently uses append-only writes (no upserts/deletes)
* **Export Formats**: Limited to dataframe and Arrow table exports
* **Remote Execution**: Does not support remote on-demand transforms
* **Spark Integration**: Direct Spark dataframe export not yet implemented

## Resources

* [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
* [PyIceberg Documentation](https://py.iceberg.apache.org/)
* [Iceberg Table Format Specification](https://iceberg.apache.org/spec/)
* [Feast Iceberg Examples](https://github.com/feast-dev/feast/tree/master/examples)
