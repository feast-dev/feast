# Iceberg Implementation Task Schedule

## Current Status: Phase 2 (IN PROGRESS)

Last updated: 2026-01-14

---

## Immediate Next Steps (Week 1)

### ⚠️ Phase 2 Blockers (COMPLETED ✅)

#### Task 2.0a: Fix IcebergDataSourceCreator Signature Mismatch ✅
**Priority**: CRITICAL  
**Status**: COMPLETED 2026-01-14  
**Time Taken**: 15 minutes

**Issue**: Parameter order in `create_data_source()` didn't match base class

**Solution Applied**:
```python
# Fixed parameter signature to match DataSourceCreator base class
def create_data_source(
    self, df, destination_name, created_timestamp_column="created_ts",
    field_mapping=None, timestamp_field=None
)
```

**File**: `sdk/python/tests/integration/feature_repos/universal/data_sources/iceberg.py:50-56`

**Result**: ✅ Method signature now matches base class, no LSP errors

---

#### Task 2.0b: Complete IcebergSource Abstract Methods ✅
**Priority**: HIGH  
**Status**: COMPLETED 2026-01-14  
**Time Taken**: 2 hours

**Implementations Completed**:

1. ✅ `get_table_column_names_and_types()` - Queries pyiceberg schema
2. ✅ `source_datatype_to_feast_value_type()` - Returns iceberg_to_feast_value_type
3. ✅ `to_proto()` / `from_proto()` - Uses CustomSourceOptions with JSON
4. ✅ `IcebergOptions.to_proto()` - JSON serialization

**File**: `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg_source.py`

**Key Technical Decisions**:
- Used `CustomSourceOptions` instead of creating new protobuf field
- JSON serialization for configuration (simple, no proto compilation needed)
- Leveraged existing `iceberg_to_feast_value_type` from type_map.py

**Result**: ✅ IcebergSource can be saved/loaded from registry

---

#### Task 2.0c: Fix IcebergRetrievalJob full_feature_names ✅
**Priority**: MEDIUM  
**Status**: COMPLETED 2026-01-14  
**Time Taken**: 45 minutes

**Changes Applied**:
1. ✅ Added `full_feature_names` parameter to `__init__`
2. ✅ Implemented feature name prefixing in query generation
3. ✅ Pass-through from `get_historical_features`

**File**: `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:113-135, 207-224`

**Result**: ✅ Feature naming follows Feast conventions

---

### Task 2.1: Run Phase 2 Checkpoint Tests (READY TO EXECUTE)
**Priority**: HIGH  
**Assignee**: TBD  
**Estimated Time**: 2-4 hours

**Steps:**
1. Install Iceberg dependencies: `uv sync --extra iceberg`
2. Run universal historical retrieval tests:
   ```bash
   uv run pytest sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py \
     -v -k "Iceberg" --tb=short
   ```
3. Document test results (pass/fail counts, error messages)
4. Identify any missing functionality

**Exit Criteria:**
- [ ] Test execution completed
- [ ] Results documented in this file
- [ ] Failure analysis completed (if any)

---

### Task 2.2: Fix Test Failures (if any)
**Priority**: HIGH  
**Assignee**: TBD  
**Estimated Time**: 4-8 hours (depends on failures)

**Common Issues to Check:**
- [ ] Entity key handling in ASOF joins
- [ ] Timestamp field mapping
- [ ] Feature name collision resolution
- [ ] NULL value handling
- [ ] Field mapping support
- [ ] TTL support in historical features

**Exit Criteria:**
- [ ] All Iceberg tests pass in universal test suite
- [ ] Phase 2 checkpoint marked as complete

---

## Short Term (Weeks 2-3)

### Task 3.1: Design Online Store Implementation
**Priority**: MEDIUM  
**Assignee**: TBD  
**Estimated Time**: 8 hours

**Deliverables:**
- [ ] Finalize partition strategy (recommend: entity_hash % 256)
- [ ] Design entity key hashing function
- [ ] Define table schema for online store
- [ ] Create ADR (Architecture Decision Record) for design choices

---

### Task 3.2: Implement IcebergOnlineStoreConfig
**Priority**: MEDIUM  
**Assignee**: TBD  
**Estimated Time**: 2 hours

**File**: `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py`

**Code Requirements:**
```python
class IcebergOnlineStoreConfig(FeastConfigBaseModel):
    type: Literal["iceberg"] = "iceberg"
    catalog_type: Optional[str] = "sql"
    catalog_name: str = "default"
    uri: Optional[str] = None
    warehouse: str = "warehouse"
    partition_strategy: Literal["entity_hash", "timestamp", "hybrid"] = "entity_hash"
    partition_count: int = 256  # for entity_hash strategy
    read_timeout_ms: int = 100
    storage_options: Dict[str, str] = Field(default_factory=dict)
```

---

### Task 3.3: Implement online_write_batch
**Priority**: MEDIUM  
**Assignee**: TBD  
**Estimated Time**: 8 hours

**Key Functions:**
1. Convert Feast EntityKeyProto + ValueProto to Arrow
2. Calculate entity_hash for partitioning
3. Append to Iceberg table
4. Handle schema evolution

**Exit Criteria:**
- [ ] Can materialize features to Iceberg online store
- [ ] Partitioning works correctly
- [ ] No data loss or corruption

---

### Task 3.4: Implement online_read
**Priority**: MEDIUM  
**Assignee**: TBD  
**Estimated Time**: 8 hours

**Key Functions:**
1. Build partition filter from entity keys
2. Execute metadata-pruned scan
3. Filter to exact entity matches
4. Select latest value per entity
5. Convert Arrow to Feast ValueProto

**Exit Criteria:**
- [ ] Can read features from Iceberg online store
- [ ] Metadata pruning is working (verify with logs)
- [ ] Returns latest value correctly

---

## Medium Term (Weeks 4-6)

### Task 3.5: Universal Online Store Tests
**Priority**: MEDIUM  
**Assignee**: TBD  
**Estimated Time**: 4 hours

**Steps:**
1. Create `IcebergOnlineStoreCreator` in tests
2. Register in `AVAILABLE_ONLINE_STORES`
3. Run universal online store tests
4. Fix any failures

---

### Task 4.1: Documentation - Offline Store
**Priority**: MEDIUM  
**Assignee**: TBD  
**Estimated Time**: 4 hours

**File**: `docs/reference/offline-stores/iceberg.md`

**Sections:**
- [ ] Overview and use cases
- [ ] Configuration examples (SQL catalog, REST catalog, AWS Glue)
- [ ] Performance characteristics
- [ ] Troubleshooting guide
- [ ] Migration from other offline stores

---

### Task 4.2: Documentation - Online Store
**Priority**: MEDIUM  
**Assignee**: TBD  
**Estimated Time**: 4 hours

**File**: `docs/reference/online-stores/iceberg.md`

**Sections:**
- [ ] Overview and trade-offs vs Redis
- [ ] Configuration examples
- [ ] Performance tuning guide
- [ ] Partition strategy selection
- [ ] Compaction best practices

---

## Long Term (Weeks 7-8)

### Task 4.3: Quickstart Guide
**Priority**: LOW  
**Assignee**: TBD  
**Estimated Time**: 4 hours

**File**: `docs/quickstart/iceberg-quickstart.md`

**Content:**
- [ ] Local setup with SQLite catalog
- [ ] AWS setup with Glue catalog
- [ ] GCP setup with REST catalog
- [ ] End-to-end example workflow

---

### Task 4.4: Performance Benchmarking
**Priority**: LOW  
**Assignee**: TBD  
**Estimated Time**: 8 hours

**Metrics to Measure:**
- [ ] Offline store: historical retrieval latency (varies by data size)
- [ ] Online store: read latency (p50, p95, p99)
- [ ] Online store: write throughput
- [ ] Storage efficiency vs Redis
- [ ] Cost comparison

---

### Task 5.1: Dependency Monitoring Setup
**Priority**: LOW  
**Assignee**: TBD  
**Estimated Time**: 2 hours

**Actions:**
- [ ] Subscribe to pyiceberg release notifications
- [ ] Subscribe to testcontainers-python release notifications
- [ ] Create GitHub issue template for dependency upgrades
- [ ] Document upgrade process

---

### Task 5.2: CI/CD Integration
**Priority**: LOW  
**Assignee**: TBD  
**Estimated Time**: 4 hours

**Steps:**
- [ ] Add Iceberg tests to CI pipeline
- [ ] Set up test data fixtures
- [ ] Configure test timeout settings
- [ ] Add to nightly test runs

---

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Upstream pyiceberg deprecations break tests | HIGH | Pin to stable version, monitor releases |
| Performance not meeting 100ms target | MEDIUM | Optimize partition strategy, add caching |
| Schema evolution issues | MEDIUM | Comprehensive testing, versioning strategy |
| Iceberg catalog downtime | LOW | Use local SQLite for tests, document HA setup |

---

## Success Metrics

### Phase 2 Complete
- [ ] All universal offline store tests pass
- [ ] Documentation updated
- [ ] Code reviewed and merged

### Phase 3 Complete
- [ ] All universal online store tests pass
- [ ] Performance benchmarks published
- [ ] Documentation complete

### Phase 4 Complete
- [ ] Community feedback incorporated
- [ ] Production deployments successful
- [ ] Maintenance runbook published

---

## Notes

- Use uv workflow for all development (`uv run`, `uv sync`, never `uv pip`)
- All deprecation warnings from upstream dependencies are documented in specs
- Phase 1 is complete, Phase 2 is in progress
- Focus on getting Phase 2 checkpoint passing before moving to Phase 3
