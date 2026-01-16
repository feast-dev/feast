---
status: pending
priority: p2
issue_id: "009"
tags: [code-review, performance, offline-store, memory]
dependencies: []
---

# Memory Materialization of File Metadata

## Problem Statement

`list(scan.plan_files())` materializes all Iceberg file metadata into memory before processing. For large tables with 10,000+ data files, this causes OOM risk.

## Findings

**Location:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:170-171, 328-329`

**Current:**
```python
tasks = list(scan.plan_files())  # Materializes all!
has_deletes = any(task.delete_files for task in tasks)
```

## Proposed Solutions

### Solution 1: Stream with Early Exit (Recommended)
**Effort:** Small
**Risk:** Low

**Implementation:**
```python
has_deletes = False
file_paths = []
for task in scan.plan_files():  # Generator
    if task.delete_files:
        has_deletes = True
        break
    file_paths.append(task.file.file_path)

if has_deletes:
    arrow_table = scan.to_arrow()
    con.register(fv.name, arrow_table)
else:
    if file_paths:
        con.execute(f"CREATE VIEW {fv.name} AS SELECT * FROM read_parquet({file_paths})")
```

## Acceptance Criteria

- [ ] Generator used instead of list()
- [ ] Early exit on MOR detection
- [ ] Memory usage O(1) instead of O(files)

## Work Log

**2026-01-16:** Identified by performance-oracle agent
