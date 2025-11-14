# Table Formats

## Overview

Table formats are metadata and transaction layers built on top of data storage formats (like Parquet). They provide advanced capabilities for managing large-scale data lakes, including ACID transactions, time travel, schema evolution, and efficient data management.

Feast supports modern table formats to enable data lakehouse architectures with your feature store.

## Supported Table Formats

### Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open table format designed for huge analytic datasets. It provides:
- **ACID transactions**: Atomic commits with snapshot isolation
- **Time travel**: Query data as of any snapshot
- **Schema evolution**: Add, drop, rename, or reorder columns safely
- **Hidden partitioning**: Partitioning is transparent to users
- **Performance**: Advanced pruning and filtering

#### Basic Configuration

```python
from feast.table_format import IcebergFormat

iceberg_format = IcebergFormat(
    catalog="my_catalog",
    namespace="my_database"
)
```

#### Configuration Options

| Parameter | Type | Description |
|-----------|------|-------------|
| `catalog` | `str` (optional) | Iceberg catalog name |
| `namespace` | `str` (optional) | Namespace/schema within the catalog |
| `properties` | `dict` (optional) | Additional Iceberg configuration properties |

#### Common Properties

```python
iceberg_format = IcebergFormat(
    catalog="spark_catalog",
    namespace="production",
    properties={
        # Snapshot selection
        "snapshot-id": "123456789",
        "as-of-timestamp": "1609459200000",  # Unix timestamp in ms

        # Performance tuning
        "read.split.target-size": "134217728",  # 128 MB splits
        "read.parquet.vectorization.enabled": "true",

        # Advanced configuration
        "io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        "warehouse": "s3://my-bucket/warehouse"
    }
)
```

#### Time Travel Example

```python
# Read from a specific snapshot
iceberg_format = IcebergFormat(
    catalog="spark_catalog",
    namespace="lakehouse"
)
iceberg_format.set_property("snapshot-id", "7896524153287651133")

# Or read as of a timestamp
iceberg_format.set_property("as-of-timestamp", "1609459200000")
```

### Delta Lake

[Delta Lake](https://delta.io/) is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. It provides:
- **ACID transactions**: Serializable isolation for reads and writes
- **Time travel**: Access and revert to earlier versions
- **Schema enforcement**: Prevent bad data from corrupting tables
- **Unified batch and streaming**: Process data incrementally
- **Audit history**: Full history of all changes

#### Basic Configuration

```python
from feast.table_format import DeltaFormat

delta_format = DeltaFormat()
```

#### Configuration Options

| Parameter | Type | Description |
|-----------|------|-------------|
| `checkpoint_location` | `str` (optional) | Location for Delta transaction log checkpoints |
| `properties` | `dict` (optional) | Additional Delta configuration properties |

#### Common Properties

```python
delta_format = DeltaFormat(
    checkpoint_location="s3://my-bucket/checkpoints",
    properties={
        # Time travel
        "versionAsOf": "5",
        "timestampAsOf": "2024-01-01 00:00:00",

        # Performance optimization
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",

        # Data skipping
        "delta.dataSkippingNumIndexedCols": "32",

        # Z-ordering
        "delta.autoOptimize.zOrderCols": "event_timestamp"
    }
)
```

#### Time Travel Example

```python
# Read from a specific version
delta_format = DeltaFormat()
delta_format.set_property("versionAsOf", "10")

# Or read as of a timestamp
delta_format = DeltaFormat()
delta_format.set_property("timestampAsOf", "2024-01-15 12:00:00")
```

### Apache Hudi

[Apache Hudi](https://hudi.apache.org/) (Hadoop Upserts Deletes and Incrementals) is a data lake storage framework for simplifying incremental data processing. It provides:
- **Upserts and deletes**: Efficient record-level updates
- **Incremental queries**: Process only changed data
- **Time travel**: Query historical versions
- **Multiple table types**: Optimize for read vs. write workloads
- **Change data capture**: Track data changes over time

#### Basic Configuration

```python
from feast.table_format import HudiFormat

hudi_format = HudiFormat(
    table_type="COPY_ON_WRITE",
    record_key="user_id",
    precombine_field="updated_at"
)
```

#### Configuration Options

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_type` | `str` (optional) | `COPY_ON_WRITE` or `MERGE_ON_READ` |
| `record_key` | `str` (optional) | Field(s) that uniquely identify a record |
| `precombine_field` | `str` (optional) | Field used to determine the latest version |
| `properties` | `dict` (optional) | Additional Hudi configuration properties |

#### Table Types

**COPY_ON_WRITE (COW)**
- Stores data in columnar format (Parquet)
- Updates create new file versions
- Best for **read-heavy workloads**
- Lower query latency

```python
hudi_format = HudiFormat(
    table_type="COPY_ON_WRITE",
    record_key="id",
    precombine_field="timestamp"
)
```

**MERGE_ON_READ (MOR)**
- Uses columnar + row-based formats
- Updates written to delta logs
- Best for **write-heavy workloads**
- Lower write latency

```python
hudi_format = HudiFormat(
    table_type="MERGE_ON_READ",
    record_key="id",
    precombine_field="timestamp"
)
```

#### Common Properties

```python
hudi_format = HudiFormat(
    table_type="COPY_ON_WRITE",
    record_key="user_id",
    precombine_field="updated_at",
    properties={
        # Query type
        "hoodie.datasource.query.type": "snapshot",  # or "incremental"

        # Incremental queries
        "hoodie.datasource.read.begin.instanttime": "20240101000000",
        "hoodie.datasource.read.end.instanttime": "20240102000000",

        # Indexing
        "hoodie.index.type": "BLOOM",

        # Compaction (for MOR tables)
        "hoodie.compact.inline": "true",
        "hoodie.compact.inline.max.delta.commits": "5",

        # Clustering
        "hoodie.clustering.inline": "true"
    }
)
```

#### Incremental Query Example

```python
# Process only new/changed data
hudi_format = HudiFormat(
    table_type="COPY_ON_WRITE",
    record_key="id",
    precombine_field="timestamp",
    properties={
        "hoodie.datasource.query.type": "incremental",
        "hoodie.datasource.read.begin.instanttime": "20240101000000",
        "hoodie.datasource.read.end.instanttime": "20240102000000"
    }
)
```

## Table Format vs File Format

It's important to understand the distinction:

| Aspect | File Format | Table Format |
|--------|-------------|--------------|
| **What it is** | Physical encoding of data | Metadata and transaction layer |
| **Examples** | Parquet, Avro, ORC, CSV | Iceberg, Delta Lake, Hudi |
| **Handles** | Data serialization | ACID, versioning, schema evolution |
| **Layer** | Storage layer | Metadata layer |

### Can be used together

```python
# Table format (metadata layer) built on top of file format (storage layer)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.table_format import IcebergFormat

iceberg = IcebergFormat(catalog="my_catalog", namespace="db")

source = SparkSource(
    name="features",
    path="catalog.db.table",
    file_format="parquet",      # Underlying storage format
    table_format=iceberg,        # Table metadata format
    timestamp_field="event_timestamp"
)
```

## Benefits of Table Formats

### Reliability
- **ACID transactions**: Ensure data consistency across concurrent operations
- **Automatic retries**: Handle transient failures gracefully
- **Schema validation**: Prevent incompatible schema changes
- **Data quality**: Constraints and validation rules

### Performance
- **Data skipping**: Read only relevant files based on metadata
- **Partition pruning**: Skip entire partitions based on predicates
- **Compaction**: Merge small files for better performance
- **Columnar pruning**: Read only necessary columns
- **Indexing**: Advanced indexing for fast lookups

### Flexibility
- **Schema evolution**: Add, remove, or modify columns without rewriting data
- **Time travel**: Access historical data states for auditing or debugging
- **Incremental processing**: Process only changed data efficiently
- **Multiple readers/writers**: Concurrent access without conflicts

## Choosing the Right Table Format

| Use Case | Recommended Format | Why |
|----------|-------------------|-----|
| Large-scale analytics with frequent schema changes | **Iceberg** | Best schema evolution, hidden partitioning, mature ecosystem |
| Streaming + batch workloads | **Delta Lake** | Unified architecture, strong integration with Spark, good docs |
| CDC and upsert-heavy workloads | **Hudi** | Efficient record-level updates, incremental queries |
| Read-heavy analytics | **Iceberg or Delta** | Excellent query performance |
| Write-heavy transactional | **Hudi (MOR)** | Optimized for fast writes |
| Multi-engine support | **Iceberg** | Widest engine support (Spark, Flink, Trino, etc.) |

## Best Practices

### 1. Choose Appropriate Partitioning
```python
# Iceberg - hidden partitioning
iceberg_format.set_property("partition-spec", "days(event_timestamp)")

# Delta - explicit partitioning in data source
# Hudi - configure via properties
hudi_format.set_property("hoodie.datasource.write.partitionpath.field", "date")
```

### 2. Enable Optimization Features
```python
# Delta auto-optimize
delta_format.set_property("delta.autoOptimize.optimizeWrite", "true")
delta_format.set_property("delta.autoOptimize.autoCompact", "true")

# Hudi compaction
hudi_format.set_property("hoodie.compact.inline", "true")
```

### 3. Manage Table History
```python
# Regularly clean up old snapshots/versions
# For Iceberg: Use expire_snapshots() procedure
# For Delta: Use VACUUM command
# For Hudi: Configure retention policies
```

### 4. Monitor Metadata Size
- Table formats maintain metadata for all operations
- Monitor metadata size and clean up old versions
- Configure retention policies based on your needs

### 5. Test Schema Evolution
```python
# Always test schema changes in non-production first
# Ensure backward compatibility
# Use proper migration procedures
```

## Data Source Support

Currently, table formats are supported with:
- [Spark data source](spark.md) - Full support for Iceberg, Delta, and Hudi

Future support planned for:
- BigQuery (Iceberg)
- Snowflake (Iceberg)
- Other data sources

## See Also

- [Spark Data Source](spark.md)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [Apache Hudi Documentation](https://hudi.apache.org/docs/overview)
- [Python API Reference - TableFormat](https://rtd.feast.dev/en/master/#feast.table_format)