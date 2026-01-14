# Iceberg Offline Store Specification

## Overview
The Iceberg Offline Store allows Feast to use Apache Iceberg tables as a source for historical feature retrieval and as a destination for materialization. This implementation focuses on a native Python experience using `pyiceberg` for table management and `duckdb` for high-performance SQL execution.

## Design Goals
- **Lightweight**: Avoid JVM and Spark dependencies where possible.
- **Catalog Flexibility**: Support "With Catalog" (REST, Glue, Hive, SQL) and "Without Catalog" (Hadoop/File-based) configurations.
- **Performance**: Use DuckDB for efficient Point-in-Time (PIT) joins on Arrow memory.
- **Cloud Native**: Support S3, GCS, and Azure Blob Storage.

## Configuration
The offline store is configured in `feature_store.yaml`:

```yaml
offline_store:
    type: iceberg
    catalog_type: rest  # rest, glue, hive, sql, or none
    catalog_name: my_catalog
    uri: http://localhost:8181
    warehouse: s3://my-bucket/warehouse
    storage_options:
        s3.endpoint: http://localhost:9000
        s3.access-key-id: minio
        s3.secret-access-key: minio123
```

## Data Source
`IcebergSource` identifies tables within the configured catalog:

```python
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import IcebergSource

source = IcebergSource(
    table_identifier="feature_db.driver_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_ts"
)
```

## Retrieval Logic (Hybrid Strategy)
1. **Filtering**: Feast identifies the required time range and entity keys.
2. **Planning**: `pyiceberg` plans the scan, identifying relevant data files and delete files.
3. **Execution Branch**:
    - **Fast Path (COW)**: If no delete files are present, extract the list of Parquet file paths. DuckDB reads these files directly (`read_parquet([...])`), enabling streaming execution and low memory footprint.
    - **Safe Path (MOR)**: If delete files are present (Merge-On-Read), execute `scan().to_arrow()` to resolve deletes in memory, then register the Arrow table in DuckDB.
4. **Join**: DuckDB registers the Entity DataFrame (as a View) and the Feature Table (View or Arrow).
5. **ASOF Join**: DuckDB executes the Point-in-Time join using its native `ASOF JOIN` capability.
6. **Output**: The result is returned as a Pandas DataFrame or Arrow Table.

## Requirements
- `pyiceberg[s3,glue,sql]`
- `duckdb`
- `pyarrow`

## Known Upstream Dependency Warnings

The following deprecation warnings originate from third-party dependencies (not from Feast code) and may appear during tests:

| Package | Warning | Status |
|---------|---------|--------|
| `testcontainers` | `@wait_container_is_ready` decorator deprecated | Internal to lib; Feast uses `wait_for_logs()` correctly |
| `pyiceberg` | `enablePackrat` → `enable_packrat` | Internal to pyiceberg parser |
| `pyiceberg` | `escChar` → `esc_char`, `unquoteResults` → `unquote_results` | Internal to pyiceberg parser |
| `pyiceberg` | Pydantic `@model_validator` mode='after' on classmethod deprecated | Internal to pyiceberg; requires pyiceberg v0.9+ |

**Action**: No code changes required in Feast. Monitor upstream releases:
- pyiceberg: https://github.com/apache/iceberg-python/releases
- testcontainers-python: https://github.com/testcontainers/testcontainers-python/releases

**Testing Note**: Use `uv run` for all test commands to ensure proper virtual environment management. See [Phase 2 checkpoint](plan.md) for test verification commands.
