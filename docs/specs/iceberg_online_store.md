# Iceberg Online Store Specification

## Overview
The Iceberg Online Store provides a "Near-line" serving mechanism for Feast. While traditional online stores like Redis offer millisecond latency, the Iceberg Online Store is designed for use cases where latency in the 500ms - 2s range is acceptable, or where features are already stored in Iceberg and the overhead of moving them to a key-value store is not justified.

## Design Goals
- **Consistency**: Use the same table format for both offline and online storage.
- **Simplicity**: No need for a separate Redis/DynamoDB cluster if sub-second latency is not required.
- **Native Implementation**: Use `pyiceberg` for efficient point-queries using metadata pruning.

## Configuration
The online store is configured in `feature_store.yaml`:

```yaml
online_store:
    type: iceberg
    catalog_type: rest
    catalog_name: online_catalog
    uri: http://localhost:8181
    warehouse: s3://my-bucket/online-warehouse
```

## Data Model
Each `FeatureView` is mapped to an Iceberg table.
- **Partitioning**: Tables are partitioned by a hash of the Entity Key to enable fast lookups.
- **Sorting**: Data is sorted within partitions by Entity Key and Event Timestamp.

## Operations
### Online Write (Materialization)
`online_write_batch` appends new feature values to the Iceberg table. 
- Note: Iceberg commits are relatively expensive. Materialization should be done in large batches or at a lower frequency (e.g., hourly).

### Online Read
`get_online_features` executes a pruned scan:
1. Feast identifies the Entity Keys requested.
2. `pyiceberg` generates a filter expression (e.g., `entity_id IN (1, 2, 3)`).
3. `pyiceberg` uses metadata (manifest files, partition stats) to read only the specific data files containing those keys.
4. The latest value for each key is returned.

## Trade-offs
| Metric | Redis | Iceberg Online |
| :--- | :--- | :--- |
| Read Latency | < 10ms | 500ms - 2s |
| Write Throughput | High | Moderate (Batch dependent) |
| Operational Complexity | High (New Cluster) | Low (Uses existing Datalake) |
| Storage Cost | High (RAM/SSD) | Low (S3/GCS) |

## Implementation Details
- Uses `pyiceberg.table.Table.scan` with `row_filter`.
- Requires `pyarrow` for processing the results of the scan.
