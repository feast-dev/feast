---
status: resolved
priority: p2
issue_id: "008"
tags: [code-review, data-integrity, offline-store]
dependencies: []
resolved_date: "2026-01-16"
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

- [x] created_timestamp_column used when available
- [x] Test with duplicate timestamps verifies correct behavior
- [x] Matches Spark/Snowflake store behavior

## Resolution

**Fixed in:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:436-439`

**Implementation:**
```python
# Rank records by timestamp descending (with created_timestamp as tiebreaker) and pick rank 1
order_by = f"{validated_timestamp} DESC"
if created_timestamp_column:
    order_by += f", {validated_created} DESC"
```

**Test Coverage:** `sdk/python/tests/unit/infra/offline_store/test_iceberg_offline_store_fixes.py::test_created_timestamp_used_in_pull_latest`

The test verifies that when `created_timestamp_column` is provided, the query includes both `event_timestamp DESC` and `created_timestamp DESC` in the ORDER BY clause, ensuring deterministic tie-breaking for records with identical timestamps.

## Work Log

**2026-01-16:** Identified by data-integrity-guardian agent
**2026-01-16:** Verified fix implementation and test coverage - marked as resolved
