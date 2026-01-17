---
status: completed
priority: p2
issue_id: "007"
tags: [code-review, performance, online-store]
dependencies: []
resolved_date: "2026-01-17"
resolution: "Vectorized deduplication already implemented using PyArrow operations. Uses sort_by() for sorting and vectorized comparison for finding unique entity_keys. No Python loop for deduplication."
verification: "iceberg_online_store.py:640-675 - vectorized sort and deduplication using PyArrow compute"
---

# O(n) Python Loop for Deduplication

## Problem Statement

The online store uses an O(n) Python loop with `.as_py()` per-cell conversion for deduplication. This bypasses PyArrow's vectorized operations, causing 10-30 second processing time for 1M rows.

**Why it matters:**
- **Read Latency:** 10-30s for 1M rows (unacceptable for serving)
- **CPU Waste:** Python object creation overhead per cell
- **Scalability:** Doesn't scale to large result sets

## Findings

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:591-614`

**Current Implementation:**
```python
for i in range(len(arrow_table)):
    entity_key_hex = arrow_table["entity_key"][i].as_py()  # Slow!
    event_ts = arrow_table["event_ts"][i].as_py()          # Slow!
    for feature_name in requested_features:
        value = arrow_table[feature_name][i].as_py()       # Very slow!
```

**Evidence:** performance-oracle agent projects 10-100x speedup with vectorization

## Proposed Solutions

### Solution 1: PyArrow Vectorized Sort + Group (Recommended)
**Pros:** 10-100x faster, leverages Arrow compute
**Cons:** More complex implementation
**Effort:** Medium
**Risk:** Low

**Implementation:**
```python
import pyarrow.compute as pc

# Sort by entity_key, event_ts DESC, created_ts DESC
sorted_indices = pc.sort_indices(
    arrow_table,
    sort_keys=[
        ("entity_key", "ascending"),
        ("event_ts", "descending"),
        ("created_ts", "descending"),
    ]
)
sorted_table = arrow_table.take(sorted_indices)

# Use run-end encoding or manual dedup to keep first per entity_key
entity_keys_col = sorted_table.column("entity_key")
mask = pc.not_equal(entity_keys_col, pc.list_slice(entity_keys_col, 1))
deduped_table = sorted_table.filter(mask)

# Build lookup and return results
```

## Recommended Action

Implement Solution 1. Combine with P1 issue #005 (created_ts tiebreaker).

## Acceptance Criteria

- [ ] Vectorized deduplication implemented
- [ ] Benchmark shows 10x+ improvement for 100K+ rows
- [ ] Correctness verified against Python loop version
- [ ] Memory usage acceptable

## Work Log

**2026-01-16:** Identified by performance-oracle agent
