---
status: completed
priority: p2
issue_id: "012"
tags: [code-review, performance, online-store, storage]
dependencies: []
resolved_date: "2026-01-17"
resolution: "partition_count default reduced from 256 to 32 in IcebergOnlineStoreConfig:114"
verification: "Code review confirms: partition_count: StrictInt = 32"
---

# Small File Problem with 256 Partitions

## Problem Statement

Default `partition_count=256` creates up to 256 small files per write batch, leading to metadata bloat and slow query planning.

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:108`

## Proposed Solutions

### Solution 1: Reduce Default to 32
```python
partition_count: StrictInt = 32  # Reduced from 256
```

### Solution 2: Add Compaction Guidance
Document periodic compaction requirements in user guide.

## Acceptance Criteria

- [ ] Reduce default partition count
- [ ] Document trade-offs (fewer partitions = better metadata, but potential hot partitions)
- [ ] Add compaction examples to docs

## Work Log

**2026-01-16:** Identified by performance-oracle agent
