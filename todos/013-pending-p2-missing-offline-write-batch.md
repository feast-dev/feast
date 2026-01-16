---
status: pending
priority: p2
issue_id: "013"
tags: [code-review, architecture, offline-store, missing-feature]
dependencies: []
---

# Missing offline_write_batch Method

## Problem Statement

`IcebergOfflineStore` doesn't implement `offline_write_batch()`, which is used by Spark and Snowflake stores for push-based feature ingestion.

**Impact:** Cannot push features to Iceberg tables programmatically.

## Proposed Solutions

### Solution 1: Implement offline_write_batch
```python
@staticmethod
def offline_write_batch(
    config: RepoConfig,
    feature_view: FeatureView,
    table: pyarrow.Table,
    progress: Optional[Callable[[int], Any]],
):
    catalog = _load_catalog(config.offline_store)
    table_id = f"{config.offline_store.namespace}.{feature_view.name}"
    
    # Get or create table
    try:
        iceberg_table = catalog.load_table(table_id)
        iceberg_table.append(table)
    except NoSuchTableError:
        # Create new table from Arrow schema
        iceberg_table = _create_table_from_arrow(catalog, table_id, table.schema)
        iceberg_table.append(table)
```

## Acceptance Criteria

- [ ] offline_write_batch implemented
- [ ] Integration test for push ingestion
- [ ] Handles schema evolution
- [ ] Documentation updated

## Work Log

**2026-01-16:** Identified by architecture-strategist agent
