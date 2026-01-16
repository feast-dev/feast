---
status: pending
priority: p1
issue_id: "005"
tags: [code-review, data-integrity, online-store, correctness]
dependencies: []
---

# Non-Deterministic Read Tie-Breaking When Timestamps Equal

## Problem Statement

The online store's read-time deduplication uses only `event_ts` for comparison. When multiple rows have identical `event_ts` values, the first row encountered wins based on arbitrary Iceberg file scan order. This leads to non-deterministic results where different reads may return different feature values.

**Why it matters:**
- **Non-Reproducibility:** Same query may return different results on different runs
- **Debugging Difficulty:** Hard to diagnose why features change unexpectedly
- **ML Model Instability:** Training/serving skew due to inconsistent features

## Findings

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:596-611`

**Current Implementation:**
```python
for i in range(len(arrow_table)):
    entity_key_hex = arrow_table["entity_key"][i].as_py()
    event_ts = arrow_table["event_ts"][i].as_py()

    if entity_key_hex in results:
        current_ts, _ = results[entity_key_hex]
        if current_ts is None or event_ts > current_ts:  # ⚠️ Strict >
            # Extract feature values
            feature_dict = {}
            # ...
            results[entity_key_hex] = (event_ts, feature_dict)
```

**Problem:** When `event_ts == current_ts`, the first row wins (based on scan order)

**Example Scenario:**
```
Row 1: entity_key=A, event_ts=2026-01-16 10:00:00, created_ts=10:00:00.001, value=X
Row 2: entity_key=A, event_ts=2026-01-16 10:00:00, created_ts=10:00:00.500, value=Y

Scan order 1 (files A, B): Returns value=X
Scan order 2 (files B, A): Returns value=X  # Still X because first in scan wins
```

**Root Cause:** No secondary tiebreaker using `created_ts`

**Evidence from data-integrity-guardian agent:**
- Iceberg doesn't guarantee file scan order
- Table compaction can change file ordering
- Other Feast stores use `created_timestamp` for tie-breaking

## Proposed Solutions

### Solution 1: Add created_ts Secondary Tiebreaker (Recommended)
**Pros:**
- Deterministic results
- Aligns with Feast semantics
- Simple fix

**Cons:**
- Assumes created_ts exists (it does in schema)
- Slightly more complex comparison

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
for i in range(len(arrow_table)):
    entity_key_hex = arrow_table["entity_key"][i].as_py()
    event_ts = arrow_table["event_ts"][i].as_py()
    created_ts = arrow_table["created_ts"][i].as_py()

    if entity_key_hex in results:
        current_ts, current_created_ts, _ = results[entity_key_hex]

        # Use created_ts as tiebreaker when event_ts is equal
        should_update = (
            current_ts is None or
            event_ts > current_ts or
            (event_ts == current_ts and created_ts is not None and
             (current_created_ts is None or created_ts > current_created_ts))
        )

        if should_update:
            feature_dict = {}
            for feature_name in requested_features:
                value = arrow_table[feature_name][i].as_py()
                if value is not None:
                    value_proto = self._python_to_value_proto(value)
                    feature_dict[feature_name] = value_proto

            results[entity_key_hex] = (
                event_ts,
                created_ts,
                feature_dict if feature_dict else None,
            )
    else:
        # First occurrence for this entity
        feature_dict = {}
        for feature_name in requested_features:
            value = arrow_table[feature_name][i].as_py()
            if value is not None:
                value_proto = self._python_to_value_proto(value)
                feature_dict[feature_name] = value_proto

        results[entity_key_hex] = (
            event_ts,
            created_ts,
            feature_dict if feature_dict else None,
        )

# Update return statement to extract only timestamp and features
return [(ts, features) for ts, _, features in results.values()]
```

### Solution 2: Use Vectorized Deduplication with Sort
**Pros:**
- Faster than Python loop
- Naturally handles tiebreaking via sort order

**Cons:**
- Larger refactor
- Should be combined with Solution 1

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
        ("created_ts", "descending"),  # Tiebreaker
    ]
)
sorted_table = arrow_table.take(sorted_indices)

# First row per entity_key is the latest (deterministic)
# ... (rest of vectorized deduplication)
```

## Recommended Action

Implement **Solution 1** immediately for correctness.

Consider **Solution 2** as part of P2 performance optimization (see todo #007).

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:596-611`

**Affected Methods:**
- `IcebergOnlineStore._convert_arrow_to_feast()`

**Schema Fields:**
- `event_ts` (TimestampType, required=False) - Primary sort key
- `created_ts` (TimestampType, required=False) - Secondary sort key

**Return Type Change:**
- Current: `List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]`
- New: Same (created_ts not exposed externally, only used internally)

## Acceptance Criteria

- [ ] `created_ts` used as tiebreaker when `event_ts` is equal
- [ ] Unit test added with:
  - Two rows: same entity_key, same event_ts, different created_ts
  - Verify row with later created_ts is returned
- [ ] Integration test verifies deterministic behavior across multiple reads
- [ ] No performance regression (tiebreaker adds minimal overhead)

## Work Log

**2026-01-16:** Issue identified during data-integrity review by data-integrity-guardian agent

## Resources

- Data integrity review: `/home/tommyk/.claude/plans/mellow-petting-kettle.md`
- Feast entity key serialization: `sdk/python/feast/protos/feast/types/EntityKey.proto`
- Similar fix in Redis store: (reference if exists)
