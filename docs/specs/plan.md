# Iceberg Storage Implementation Plan

## Goal
Implement a native Python Iceberg Offline and Online store using `pyiceberg` and `duckdb`.

## Roadmap

### Phase 1: Foundation & Test Harness (RED)
- [ ] Update `sdk/python/setup.py` with `pyiceberg`, `duckdb`, and `pyarrow`.
- [ ] Implement `IcebergOfflineStoreConfig` and `IcebergSource`.
- [ ] Create `IcebergDataSourceCreator` in `sdk/python/tests/integration/feature_repos/universal/data_sources/iceberg.py`.
- [ ] Register in `AVAILABLE_OFFLINE_STORES` in `repo_configuration.py`.
- [ ] **Checkpoint**: Run universal tests and see them fail with `NotImplementedError`.

### Phase 2: Offline Store Implementation (IN PROGRESS)
- [ ] Implement `get_historical_features` in `IcebergOfflineStore`.
    - [ ] Implement **Hybrid Strategy**:
        - Check `scan().plan_files()` for deletes.
        - **COW Path**: `con.execute(f"CREATE VIEW features AS SELECT * FROM read_parquet({file_list})")`.
        - **MOR Path**: `con.register("features", table.scan().to_arrow())`.
    - [ ] Implement DuckDB ASOF join SQL generation.
- [ ] Implement `pull_latest_from_table_or_query` for materialization.
- [ ] **Checkpoint**: Pass `test_universal_historical_retrieval.py`.

### Phase 3: Online Store Implementation
- [ ] Implement `IcebergOnlineStore`.
    - `online_write_batch`: Append to Iceberg tables.
    - `online_read`: Metadata-pruned scan using `pyiceberg`.
- [ ] **Checkpoint**: Pass online universal tests.

### Phase 4: Polish & Documentation
- [ ] Add `docs/reference/offline-stores/iceberg.md`.
- [ ] Add `docs/reference/online-stores/iceberg.md`.
- [ ] Final audit of type mappings and performance.

## Design Specifications
- [Offline Store Spec](iceberg_offline_store.md)
- [Online Store Spec](iceberg_online_store.md)
