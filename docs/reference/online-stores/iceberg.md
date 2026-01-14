# Iceberg online store

## Description

The Iceberg online store provides a "near-line" serving option using [Apache Iceberg](https://iceberg.apache.org/) tables with [PyIceberg](https://py.iceberg.apache.org/). It trades some latency (50-100ms) for significant operational simplicity and cost efficiency compared to traditional in-memory stores like Redis.

**Key Features:**
* Native Iceberg table format for online serving
* Metadata-based partition pruning for efficient lookups
* Entity hash partitioning for single-partition reads
* Support for multiple catalog types (REST, Glue, Hive, SQL)
* No separate infrastructure - reuses existing Iceberg catalog
* Object storage cost vs in-memory cost (orders of magnitude cheaper)
* Batch-oriented writes for materialization efficiency

**Performance Characteristics:**
* Read latency (p95): 50-100ms (vs <10ms for Redis)
* Write throughput: Batch-dependent (1000-10000 records/sec)
* Storage cost: Object storage (S3/GCS) vs RAM/SSD
* Operational complexity: Low (reuses Iceberg catalog)

**Trade-offs:**
* ✅ **Use for**: Near-line serving, hourly/daily feature updates, cost-sensitive deployments, development/testing
* ❌ **Don't use for**: Ultra-low latency requirements (<10ms), real-time streaming, transactional consistency

## Getting started

In order to use this online store, you'll need to install the Iceberg dependencies:

```bash
uv sync --extra iceberg
```

Or if using pip:
```bash
pip install 'feast[iceberg]'
```

This installs:
* `pyiceberg[sql,duckdb]>=0.8.0` - Native Iceberg table operations
* `duckdb>=1.0.0` - SQL engine support

## Example

### Basic Configuration (REST Catalog)

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.IcebergOfflineStore
    catalog_type: rest
    catalog_name: feast_catalog
    uri: http://localhost:8181
    warehouse: s3://my-bucket/warehouse
online_store:
    type: iceberg
    catalog_type: rest
    catalog_name: feast_catalog
    uri: http://localhost:8181
    warehouse: s3://my-bucket/warehouse
    namespace: feast_online
    partition_strategy: entity_hash
    partition_count: 256
    read_timeout_ms: 100
    storage_options:
        s3.endpoint: http://localhost:9000
        s3.access-key-id: minio
        s3.secret-access-key: minio123
```
{% endcode %}

### AWS Glue Catalog Configuration

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
    type: iceberg
    catalog_type: glue
    catalog_name: feast_catalog
    warehouse: s3://my-bucket/warehouse
    namespace: feast_online
    partition_strategy: entity_hash
    partition_count: 256
    storage_options:
        s3.region: us-west-2
        s3.access-key-id: ${AWS_ACCESS_KEY_ID}
        s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
```
{% endcode %}

### Local Development (SQL Catalog)

{% code title="feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
    type: iceberg
    catalog_type: sql
    catalog_name: feast_catalog
    uri: sqlite:///data/iceberg_catalog.db
    warehouse: data/warehouse
    namespace: feast_online
    partition_strategy: entity_hash
    partition_count: 64
```
{% endcode %}

## Configuration Options

The full set of configuration options is available in `IcebergOnlineStoreConfig`:

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `type` | str | Yes | `"iceberg"` | Must be `iceberg` or full class path |
| `catalog_type` | str | Yes | `"rest"` | Type of Iceberg catalog: `rest`, `glue`, `hive`, `sql` |
| `catalog_name` | str | Yes | `"feast_catalog"` | Name of the Iceberg catalog |
| `uri` | str | No | - | Catalog URI (required for REST/SQL catalogs) |
| `warehouse` | str | Yes | `"warehouse"` | Warehouse path (S3/GCS/local path) |
| `namespace` | str | No | `"feast_online"` | Iceberg namespace for online tables |
| `partition_strategy` | str | No | `"entity_hash"` | Partitioning strategy: `entity_hash`, `timestamp`, `hybrid` |
| `partition_count` | int | No | `256` | Number of partitions for hash-based partitioning |
| `read_timeout_ms` | int | No | `100` | Timeout for online reads in milliseconds |
| `storage_options` | dict | No | `{}` | Additional storage configuration (e.g., S3 credentials) |

## Partition Strategies

Choosing the right partition strategy is critical for performance:

### Entity Hash Partitioning (Recommended)

```yaml
partition_strategy: entity_hash
partition_count: 256
```

* **Use Case**: Fast single-entity lookups
* **How it works**: Partitions by hash of entity key(s) modulo partition_count
* **Performance**: Single-partition reads via metadata pruning
* **Best for**: High-cardinality entity spaces, random access patterns

### Timestamp Partitioning

```yaml
partition_strategy: timestamp
```

* **Use Case**: Time-range queries, time-series analysis
* **How it works**: Partitions by hour of event_timestamp
* **Performance**: Good for temporal queries, less efficient for entity lookups
* **Best for**: Chronological access patterns, batch processing

### Hybrid Partitioning

```yaml
partition_strategy: hybrid
partition_count: 64
```

* **Use Case**: Balanced workload (entity lookups + time ranges)
* **How it works**: Partitions by both entity_hash (64 buckets) and day(event_timestamp)
* **Performance**: Moderate overhead, flexible access patterns
* **Best for**: Mixed workloads, when both entity and time filters are common

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Iceberg online store.

| | Iceberg |
| :-------------------------------------------------------- | :------ |
| write feature values to the online store                  | yes     |
| read feature values from the online store                 | yes     |
| update infrastructure (e.g. tables) in the online store   | yes     |
| teardown infrastructure (e.g. tables) in the online store | yes     |
| generate a plan of infrastructure changes                 | no      |
| support for on-demand transforms                          | yes     |
| readable by Python SDK                                    | yes     |
| readable by Java                                          | no      |
| readable by Go                                            | no      |
| support for entityless feature views                      | yes     |
| support for concurrent writing to the same key            | no      |
| support for ttl (time to live) at retrieval               | no      |
| support for deleting expired data                         | no      |
| collocated by feature view                                | yes     |
| collocated by feature service                             | no      |
| collocated by entity key                                  | yes     |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).

## Performance Optimization

### Metadata Pruning (Critical!)

Iceberg's metadata layer enables partition filtering **before** reading data files:

```python
# With entity_hash partitioning:
# 1. Compute entity hash: hash(entity_key) % 256
# 2. Filter to single partition via metadata
# 3. Read only relevant Parquet files
# Result: 20-50ms instead of 500-2000ms
```

### Column Projection

Only requested feature columns are read from Parquet files:

```python
store.get_online_features(
    features=["driver_hourly_stats:conv_rate"],  # Only reads conv_rate column
    entity_rows=[{"driver_id": 1001}],
)
```

### Batch Writes

Write features in large batches for optimal throughput:

```python
# Good: Large batches (1000-10000 records)
store.materialize_incremental(
    end_date=datetime.now(),
)

# Avoid: Small individual writes (high latency)
```

### Z-Ordering (Future)

Within partitions, sort by entity key for faster scans (future enhancement).

## Read Path Details

When reading features from the online store:

1. **Compute Entity Hash**: Hash entity keys to partition IDs
2. **Metadata Pruning**: Filter partitions using Iceberg metadata
3. **Scan Filtered Files**: Read only relevant Parquet files
4. **Latest Record Selection**: Post-scan filtering to get most recent values
5. **Type Conversion**: Convert Arrow data to Feast ValueProto

## Write Path Details

When writing features to the online store:

1. **Convert to Arrow**: Transform Feast data to Arrow table format
2. **Add Partition Columns**: Compute entity_hash and partition values
3. **Append to Iceberg**: Batch append to Iceberg table
4. **Commit Metadata**: Update Iceberg metadata (relatively expensive)

**Performance Tip**: Materialize in large batches to amortize commit overhead.

## Performance Comparison

| Metric | Iceberg Online | Redis | SQLite |
|--------|---------------|-------|--------|
| Read Latency (p50) | 30-50ms | 1-5ms | 10-20ms |
| Read Latency (p95) | 50-100ms | 5-10ms | 20-50ms |
| Write Throughput | 1K-10K/sec (batch) | High | Moderate |
| Storage Cost | $0.023/GB/mo (S3) | $0.10-1.00/GB/mo (RAM) | Disk-based |
| Ops Complexity | Low | High | Low |
| Scalability | Excellent | Good | Limited |

## Best Practices

### 1. Choose the Right Partition Count

```yaml
# High cardinality entities (millions): 256 or 512 partitions
partition_count: 256

# Medium cardinality (thousands): 64 partitions
partition_count: 64

# Low cardinality (hundreds): 16 partitions
partition_count: 16
```

### 2. Batch Materialization

```python
# Schedule periodic materialization (hourly/daily)
feast materialize-incremental 2024-01-15T00:00:00
```

### 3. Monitor Metadata Size

```bash
# Periodically compact small files to maintain performance
# Use Iceberg's maintenance procedures
```

### 4. Storage Credentials

```yaml
# Use environment variables for secrets
storage_options:
    s3.access-key-id: ${AWS_ACCESS_KEY_ID}
    s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
```

### 5. Separate Namespaces

```yaml
# Use different namespaces for offline and online stores
offline_store:
    namespace: feast_offline

online_store:
    namespace: feast_online
```

## Use Cases

### ✅ Ideal For

* **Near-line serving**: Features updated hourly or daily
* **Cost-sensitive deployments**: Avoiding Redis infrastructure costs
* **Analytical serving**: Hybrid OLAP/OLTP workloads
* **Archival with serving**: Serve historical features directly
* **Development/testing**: Simpler setup than Redis

### ❌ Not Suitable For

* **Ultra-low latency**: Requirements <10ms
* **Real-time streaming**: Millisecond-level updates
* **Transactional consistency**: ACID guarantees needed
* **High write frequency**: Continuous micro-batch writes

## Limitations

* **Higher Latency**: 50-100ms vs 1-10ms for Redis
* **Write Amplification**: Each write creates new Parquet file (mitigated by batching)
* **No Transactions**: Eventual consistency model
* **Compaction Required**: Periodic compaction needed for performance
* **No TTL**: Time-to-live not implemented (manual cleanup required)

## Catalog Types

### REST Catalog (Recommended)

```yaml
catalog_type: rest
uri: http://iceberg-rest:8181
```

### AWS Glue

```yaml
catalog_type: glue
storage_options:
    s3.region: us-west-2
```

### Hive Metastore

```yaml
catalog_type: hive
uri: thrift://hive-metastore:9083
```

### SQL Catalog (Local Dev)

```yaml
catalog_type: sql
uri: sqlite:///data/iceberg_catalog.db
```

## Cloudflare R2 Configuration

Cloudflare R2 is an excellent choice for Iceberg online store with its S3-compatible API and cost-effective pricing:

### Using R2 with S3-Compatible Storage

```yaml
online_store:
    type: feast.infra.online_stores.contrib.iceberg_online_store.iceberg.IcebergOnlineStore
    catalog_type: sql  # or rest, hive, glue
    catalog_name: r2_online_catalog
    uri: postgresql://user:pass@catalog-host:5432/online_catalog
    warehouse: s3://my-r2-bucket/online_warehouse
    namespace: online
    partition_strategy: entity_hash
    storage_options:
        s3.endpoint: https://<account-id>.r2.cloudflarestorage.com
        s3.access-key-id: ${R2_ACCESS_KEY_ID}
        s3.secret-access-key: ${R2_SECRET_ACCESS_KEY}
        s3.region: auto
        s3.force-virtual-addressing: true  # Required for R2
```

**R2-Specific Configuration:**
- `s3.force-virtual-addressing: true` is **mandatory** for R2
- R2 endpoint: `https://<account-id>.r2.cloudflarestorage.com`
- Use environment variables for credentials (never commit secrets)
- `s3.region: auto` works with R2's global architecture

### Using R2 Data Catalog (Beta)

```yaml
online_store:
    type: feast.infra.online_stores.contrib.iceberg_online_store.iceberg.IcebergOnlineStore
    catalog_type: rest
    catalog_name: r2_data_catalog
    uri: <r2-catalog-uri>  # From R2 Data Catalog
    warehouse: <r2-warehouse-name>
    namespace: online
    partition_strategy: entity_hash
    storage_options:
        token: ${R2_DATA_CATALOG_TOKEN}
```

### R2 Performance Optimization

For optimal performance with R2:

1. **Partition Strategy**: Use `entity_hash` to distribute data across R2's global network
2. **Batch Writes**: Materialize in larger batches to reduce API calls
3. **Caching**: Enable local caching for frequently accessed entities
4. **Compaction**: Run periodic compaction to optimize file sizes

```python
# Batch materialization for R2
fs.materialize_incremental(
    end_date=datetime.now(),
    feature_views=[driver_hourly_stats],
)
```

**R2 Benefits for Online Store:**
- **No egress fees**: Cost-effective for high-traffic applications
- **Global caching**: Fast reads from edge locations
- **S3 compatibility**: Works with all Iceberg tooling
- **Predictable pricing**: No per-request charges

**Resources:**
- [Cloudflare R2 Docs](https://developers.cloudflare.com/r2/)
- [R2 Performance Best Practices](https://developers.cloudflare.com/r2/reference/performance/)

## Monitoring

Key metrics to monitor:

* **Read latency percentiles** (p50, p95, p99)
* **Metadata file size** (affects read latency)
* **Number of data files per partition** (compaction trigger)
* **Storage cost** (S3/GCS object count and size)

## Resources

* [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
* [PyIceberg Documentation](https://py.iceberg.apache.org/)
* [Iceberg Partitioning](https://iceberg.apache.org/docs/latest/partitioning/)
* [Feast Online Stores Overview](overview.md)
