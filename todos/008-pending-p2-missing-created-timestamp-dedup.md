---
status: pending
priority: p2
issue_id: "008"
tags: [code-review, data-integrity, offline-store]
dependencies: []
---

# Missing created_timestamp Deduplication in Offline Store

## Problem Statement

`pull_latest_from_table_or_query()` uses ROW_NUMBER windowing with only `timestamp_field` for ordering. When multiple records have the same timestamp, ordering is non-deterministic. The `created_timestamp_column` should be used as a secondary sort key.

**Why it matters:**
- **Non-Reproducible Results:** Different runs may return different records
- **Incorrect "Latest" Selection:** May not select the truly latest record

## Findings

**Location:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:273-278`

**Current Implementation:**
```python
query = f"""
SELECT {columns_str} FROM (
    SELECT *, row_number() OVER (PARTITION BY {join_keys_str} ORDER BY {timestamp_field} DESC) as rn
    FROM {source_table}
) WHERE rn = 1
"""
```

**Missing:** `created_timestamp_column` in ORDER BY

## Proposed Solutions

### Solution 1: Add created_timestamp to ORDER BY (Recommended)
**Effort:** Small
**Risk:** Low

**Implementation:**
```python
order_by = f"{timestamp_field} DESC"
if created_timestamp_column:
    order_by += f", {created_timestamp_column} DESC"

query = f"""
SELECT {columns_str} FROM (
    SELECT *, row_number() OVER (PARTITION BY {join_keys_str} ORDER BY {order_by}) as rn
    FROM {source_table}
) WHERE rn = 1
"""
```

## Acceptance Criteria

- [ ] created_timestamp_column used when available
- [ ] Test with duplicate timestamps verifies correct behavior
- [ ] Matches Spark/Snowflake store behavior

## Work Log

**2026-01-16:** Identified by data-integrity-guardian agent
