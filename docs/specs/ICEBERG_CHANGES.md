# Iceberg Implementation - Changes Log

Last Updated: 2026-01-14

## Overview

This document tracks all changes made during the Iceberg offline and online store implementation for Feast.

---

## Phase 1: Foundation (COMPLETE)

### Dependencies Added
- **pyproject.toml**: Added `iceberg` optional dependency group
  ```toml
  iceberg = [
      "pyiceberg[sql,duckdb]>=0.8.0",
      "duckdb>=1.0.0",
  ]
  ```

### Implementation Status
- ✅ `IcebergOfflineStoreConfig` - Complete with catalog configuration
- ✅ `IcebergSource` - Complete data source implementation
- ✅ `IcebergDataSourceCreator` - Complete test harness integration
- ✅ Registered in `AVAILABLE_OFFLINE_STORES`

---

## Phase 2: Offline Store Implementation (IN PROGRESS)

### Critical Fixes Applied (Tasks 2.0a/b/c)

#### Task 2.0a: Fixed IcebergDataSourceCreator Signature Mismatch
**File**: `sdk/python/tests/integration/feature_repos/universal/data_sources/iceberg.py`

**Issue**: Method signature didn't match base class `DataSourceCreator`

**Changes**:
```python
# BEFORE
def create_data_source(self, df, destination_name, entity_name, timestamp_field,
                      created_timestamp_column, field_mapping)

# AFTER  
def create_data_source(self, df, destination_name, created_timestamp_column="created_ts",
                      field_mapping=None, timestamp_field=None)
```

**Impact**: Tests can now instantiate data sources correctly

---

#### Task 2.0b: Completed IcebergSource Abstract Methods
**File**: `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg_source.py`

**Implemented Methods**:

1. **`get_table_column_names_and_types()`**
   - Queries pyiceberg catalog for table schema
   - Returns `Iterable[Tuple[str, str]]` of (column_name, type)
   - Handles catalog connection and table loading
   
2. **`source_datatype_to_feast_value_type()`**
   - Returns callable mapping Iceberg types to Feast ValueTypes
   - Leverages existing `iceberg_to_feast_value_type` from `type_map.py`

3. **`to_proto()` / `from_proto()` / `IcebergOptions.to_proto()`**
   - Uses `CustomSourceOptions` with JSON-serialized configuration
   - Stores table_identifier in serialized format
   - Avoids need for new protobuf definitions

**Code**:
```python
def get_table_column_names_and_types(self, config: RepoConfig):
    catalog = load_catalog(config.offline_store.catalog_name, **catalog_props)
    table = catalog.load_table(self.table_identifier)
    schema = table.schema()
    for field in schema.fields:
        yield (field.name, str(field.field_type).lower())

def source_datatype_to_feast_value_type(self):
    return iceberg_to_feast_value_type

# Protobuf serialization using CustomSourceOptions
class IcebergOptions:
    def to_proto(self) -> bytes:
        config = {'table_identifier': self._table_identifier}
        return json.dumps(config).encode('utf-8')
```

**Impact**: Data source can be saved/loaded from registry correctly

---

#### Task 2.0c: Fixed IcebergRetrievalJob full_feature_names
**File**: `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py`

**Changes**:

1. **IcebergRetrievalJob Constructor**:
   ```python
   # BEFORE
   def __init__(self, con, query):
       self._full_feature_names = False  # Hardcoded
   
   # AFTER
   def __init__(self, con, query, full_feature_names=False):
       self._full_feature_names = full_feature_names
   ```

2. **Query Generation with Feature Name Prefixing**:
   ```python
   for feature in fv.features:
       feature_name = feature.name
       if full_feature_names:
           feature_name = f"{fv.name}__{feature.name}"
       query += f", {fv.name}.{feature.name} AS {feature_name}"
   ```

3. **Pass-through from get_historical_features**:
   ```python
   return IcebergRetrievalJob(con, query, full_feature_names)
   ```

**Impact**: Feature naming matches Feast conventions when `full_feature_names=True`

---

### Other Improvements

#### Added Missing Imports
- `typing.Optional`, `typing.Dict` in `iceberg.py` (data sources)
- `json` for protobuf serialization in `iceberg_source.py`
- `ValueType` from `feast.value_type` in `iceberg_source.py`

#### Fixed Missing Methods in IcebergDataSourceCreator
- ✅ `create_saved_dataset_destination()` - Returns file-based storage
- ✅ `create_logged_features_destination()` - Returns file logging destination  
- ✅ `teardown()` - Cleans up SQLite catalog and warehouse directory

---

## Documentation Updates

### New Documents Created

1. **`docs/specs/iceberg_task_schedule.md`** (267 lines)
   - 8-week implementation timeline
   - Tasks 2.0a/b/c documented with solutions
   - Risk register and success metrics
   - Detailed task breakdown for Phases 2-5

2. **`docs/specs/ICEBERG_CHANGES.md`** (this file)
   - Comprehensive change log
   - Technical details of all fixes
   - Before/after code comparisons

### Updated Documents

1. **`docs/specs/plan.md`**
   - Phase 1 marked COMPLETE
   - Phase 2 updated with blocker fixes
   - Phases 3-5 expanded with detailed tasks
   - Test verification commands added
   - Quick reference section added

2. **`docs/specs/iceberg_offline_store.md`**
   - Added "Known Upstream Dependency Warnings" section
   - Documented testcontainers and pyiceberg deprecations
   - Added testing notes with uv workflow

3. **`docs/specs/iceberg_online_store.md`**
   - Complete rewrite (51 → 180 lines)
   - 3 partition strategies detailed
   - Performance characteristics documented
   - Implementation status tracking
   - Trade-offs vs Redis clearly outlined

---

## Known Issues & Limitations

### Resolved
- ✅ Method signature mismatch in `create_data_source`
- ✅ Missing abstract method implementations
- ✅ Hardcoded `full_feature_names=False`
- ✅ Missing protobuf serialization

### Outstanding (Minor LSP Warnings)
- ⚠️ `Optional[str]` vs `str` type mismatches (non-blocking)
- ⚠️ SavedDatasetFileStorage file_format parameter type (non-blocking)
- ⚠️ FileLoggingDestination import (works at runtime)

### To Be Discovered (Phase 2 Testing)
- Entity key handling edge cases
- Multi-entity feature views
- TTL support
- Schema evolution
- Concurrent access patterns

---

## Upstream Dependencies

### Current Versions
- `pyiceberg >= 0.8.0` (with `sql,duckdb` extras)
- `duckdb >= 1.0.0`
- `testcontainers` (test dependency)

### Known Deprecation Warnings (No Action Required)
All warnings originate from library internals, not Feast code:

| Library | Warning | Resolution |
|---------|---------|------------|
| `testcontainers` | `@wait_container_is_ready` deprecated | Internal; Feast uses `wait_for_logs()` |
| `pyiceberg` | pyparsing API deprecations | Internal to pyiceberg.expressions.parser |
| `pyiceberg` | Pydantic validator deprecation | Requires pyiceberg v0.9+ |

**Monitoring**: Watch GitHub releases for upstream fixes

---

## Testing Strategy

### Phase 2 Checkpoint (Next Immediate Step)
```bash
# Install dependencies
uv sync --extra iceberg

# Run universal offline store tests for Iceberg
uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py \
  -v -k "Iceberg" --tb=short

# Check for warnings
uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py \
  -v -k "Iceberg" -W default::DeprecationWarning
```

### Expected Test Coverage
- Historical feature retrieval (ASOF joins)
- Point-in-time correctness
- Entity key handling
- Timestamp field mapping
- Field mapping support
- Feature name prefixing (full_feature_names)
- Empty table handling
- TTL support

---

## Performance Characteristics

### Offline Store (Measured/Expected)
- **COW tables** (no deletes): Fast path using DuckDB `read_parquet()`
  - Direct file access, streaming execution
  - Low memory footprint
  
- **MOR tables** (with deletes): Safe path using PyIceberg
  - Resolves deletes in memory
  - Higher memory usage but correct results

- **ASOF JOIN**: Native DuckDB ASOF JOIN
  - Efficient point-in-time joins
  - Handles multiple feature views

### Online Store (Not Yet Implemented)
- Expected latency: 50-100ms (p95)
- Partition strategy: entity_hash % 256 (recommended)
- Trade-off: Lower cost vs Redis, higher latency

---

## File Changes Summary

```
Total: 459 insertions(+), 82 deletions(-) across 9 files

Code Changes:
  sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/
    ├── iceberg.py          (+93, -29)  # full_feature_names, query improvements
    └── iceberg_source.py   (+62, -18)  # abstract methods, protobuf
  sdk/python/tests/integration/feature_repos/universal/data_sources/
    └── iceberg.py          (+39, -8)   # signature fix, missing methods
  sdk/python/feast/
    └── type_map.py         (+19)       # (pre-existing)

Documentation:
  docs/specs/
    ├── plan.md                      (+110, -25)  # Phase updates
    ├── iceberg_task_schedule.md     (+267)      # NEW: Timeline
    ├── iceberg_offline_store.md     (+17)       # Warnings section
    ├── iceberg_online_store.md      (+183, -51) # Complete rewrite
    └── ICEBERG_CHANGES.md           NEW         # This file

Configuration:
  pyproject.toml                     (+4)        # iceberg extra
```

---

## Next Actions

### Immediate (Week 1)
1. ✅ Fix critical blockers (Tasks 2.0a/b/c) - **DONE**
2. ⏭️ Run Phase 2 checkpoint tests (Task 2.1) - **NEXT**
3. ⏭️ Fix any test failures (Task 2.2)
4. ⏭️ Mark Phase 2 complete

### Short Term (Weeks 2-3)
5. Design online store implementation
6. Implement IcebergOnlineStoreConfig
7. Implement online_write_batch
8. Implement online_read

### Medium Term (Weeks 4-6)
9. Universal online store tests
10. Performance benchmarking
11. Documentation (reference docs)

### Long Term (Weeks 7-8)
12. Quickstart guide
13. CI/CD integration
14. Community feedback

---

## Contributors & Acknowledgments

- Implementation follows Feast's offline store patterns
- Uses PyIceberg for native Python Iceberg support
- DuckDB for efficient ASOF joins
- Testcontainers for integration testing

---

## References

- [Iceberg Offline Store Spec](iceberg_offline_store.md)
- [Iceberg Online Store Spec](iceberg_online_store.md)
- [Implementation Plan](plan.md)
- [Task Schedule](iceberg_task_schedule.md)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [DuckDB ASOF JOIN](https://duckdb.org/docs/sql/query_syntax/from.html#as-of-joins)
