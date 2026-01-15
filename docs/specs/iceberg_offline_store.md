# Iceberg Offline Store Specification

## Overview
The Iceberg Offline Store allows Feast to use Apache Iceberg tables as a source for historical feature retrieval and as a destination for materialization. This implementation focuses on a native Python experience using `pyiceberg` for table management and `duckdb` for high-performance SQL execution.

## Implementation Status

✅ **COMPLETE** - All phases finished 2026-01-14

**Phase Summary**:
- ✅ Phase 2: Core offline store implementation (Commit: 0093113d9)
- ✅ Phase 5.1: Bug fixes - duplicate query building (Commit: 8ce4bd85f)
- ✅ Phase 5.2: Integration tests (Commit: d54624a1c)

**Files Implemented**:
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py` (232 lines)
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg_source.py` (132 lines)
- `sdk/python/tests/integration/feature_repos/universal/data_sources/iceberg.py` (164 lines)
- `sdk/python/tests/integration/offline_store/test_iceberg_offline_store.py` (196 lines)

**Test Coverage**:
- 5 comprehensive integration tests (point-in-time correctness, multi-entity joins, schema inference, edge cases)
- Universal test framework integration (IcebergDataSourceCreator)
- No external dependencies (SQLite catalog, local filesystem)

**Documentation**:
- User guide: `docs/reference/offline-stores/iceberg.md` (344 lines with R2 section)
- Quickstart: `docs/specs/iceberg_quickstart.md` (479 lines)
- Local example: `examples/iceberg-local/` (4 files, 581 lines)

## Design Goals
- **Lightweight**: Avoid JVM and Spark dependencies where possible. ✅
- **Catalog Flexibility**: Support "With Catalog" (REST, Glue, Hive, SQL) configurations. ✅
- **Performance**: Use DuckDB for efficient Point-in-Time (PIT) joins on Arrow memory. ✅
- **Cloud Native**: Support S3, GCS, and Azure Blob Storage. ✅

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

## Final Implementation Details (Updated 2026-01-15)

- **Engine**: DuckDB for efficient SQL execution and temporal joins.
- **Join Strategy**: DuckDB `ASOF JOIN` for point-in-time correctness.
- **Read Strategy**: Hybrid COW/MOR detection based on Iceberg delete file manifests.
- **Interface**: Implemented both `pull_latest_from_table_or_query` and `pull_all_from_table_or_query` as `@staticmethod` to match Feast 0.38+ requirements.
- **Catalog Support**: Explicit support for REST, SQL, Hive, and Glue catalogs.
- **Dependencies**: `pyiceberg[sql,duckdb]>=0.8.0`, `duckdb>=1.0.0`.

### Technical Stats
- **Implementation**: 285 lines
- **Tests**: 196 lines (5 tests)
- **Status**: Production Ready ✅

## Requirements
- `pyiceberg[sql,duckdb]>=0.8.0` (plus catalog/storage extras as needed: `s3`, `glue`, `hive`)
- `duckdb>=1.0.0`
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
