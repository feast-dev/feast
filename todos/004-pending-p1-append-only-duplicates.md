---
status: pending
priority: p1
issue_id: "004"
tags: [code-review, data-integrity, online-store, storage-cost, performance]
dependencies: []
---

# Append-Only Writes Create Unbounded Duplicate Rows

## Problem Statement

The Iceberg online store uses `iceberg_table.append()` for writes, which never removes or updates existing rows. Every materialization creates new rows for the same entity keys, causing unbounded storage growth and degraded read performance over time.

**Why it matters:**
- **Storage Cost:** Unbounded growth leads to massive storage costs
- **Read Performance:** More duplicate rows = slower queries
- **Operational Burden:** Requires manual compaction/cleanup
- **Not True Upsert:** Unlike Redis/DynamoDB which replace values

## Findings

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:167-168`

**Current Implementation:**
```python
# Append to Iceberg table (never removes old rows)
iceberg_table.append(arrow_table)
```

**Impact Over Time:**
```
Materialization 1: Entity A -> value1 (1 row)
Materialization 2: Entity A -> value2 (2 rows total)
Materialization 3: Entity A -> value3 (3 rows total)
...
Materialization N: Entity A -> valueN (N rows total)
```

**Evidence from agents:**
- Performance-oracle: Read-time deduplication becomes O(n) bottleneck
- Data-integrity-guardian: Storage grows unbounded without cleanup
- Architecture-strategist: Deviates from Redis/DynamoDB true upsert pattern

**Comparison with Other Stores:**
| Store | Write Semantics | Duplicates? |
|-------|-----------------|-------------|
| Redis | HSET (upsert) | No |
| DynamoDB | PutItem (upsert) | No |
| Iceberg (current) | Append-only | Yes |

## Proposed Solutions

### Solution 1: Document + Compaction Guidance (Short-term)
**Pros:**
- No code changes required
- Simple to implement immediately
- Aligns with Iceberg best practices

**Cons:**
- Doesn't solve the problem
- Requires manual intervention
- Users may not know to compact

**Effort:** Small
**Risk:** Low

**Implementation:**
```markdown
# In docs/reference/online-stores/iceberg.md

## Important: Compaction Required

The Iceberg online store uses append-only writes. **You must run periodic
compaction to prevent unbounded storage growth.**

### Compaction Strategy

```python
# Weekly compaction job
from pyiceberg.catalog import load_catalog

catalog = load_catalog("feast_catalog", ...)
table = catalog.load_table("feast_online.customer_features")

# Remove duplicate rows, keeping only the latest per entity_key
table.rewrite_data_files(
    target_file_size_bytes=128 * 1024 * 1024,  # 128MB
)

# Vacuum old files
table.expire_snapshots(older_than=datetime.now() - timedelta(days=7))
```
```

### Solution 2: Implement True Upsert with Iceberg Merge (Recommended)
**Pros:**
- True upsert semantics like Redis/DynamoDB
- No duplicate rows
- No manual compaction required

**Cons:**
- More complex implementation
- Requires Iceberg 1.4+ for merge support
- Slower writes (read-modify-write)

**Effort:** Medium
**Risk:** Medium

**Implementation:**
```python
def online_write_batch(self, ...):
    # Convert to Arrow table
    arrow_table = self._convert_feast_to_arrow(...)

    # Use Iceberg merge/overwrite instead of append
    # Option A: Overwrite partitions (efficient for partition isolation)
    iceberg_table.overwrite(
        arrow_table,
        overwrite_filter=...,  # Filter to entity_hash partitions
    )

    # Option B: Merge (requires Iceberg 1.4+)
    # PyIceberg doesn't yet support merge, may need custom implementation
```

### Solution 3: Automatic Compaction on Read
**Pros:**
- Transparent to users
- Ensures reads are efficient

**Cons:**
- Adds latency to reads
- Complex concurrency handling
- May conflict with writes

**Effort:** Large
**Risk:** High

## Recommended Action

**Immediate (P1):**
- Implement **Solution 1**: Add clear documentation warning about storage growth
- Include example compaction scripts in `docs/reference/online-stores/iceberg.md`
- Add warning log on first write

**Follow-up (P2):**
- Research Iceberg merge/overwrite patterns for true upsert
- Prototype **Solution 2** in next release

**Code Addition:**
```python
# Add to online_write_batch
if not hasattr(self, '_compaction_warning_shown'):
    logger.warning(
        f"Iceberg online store uses append-only writes. "
        f"Run periodic compaction to prevent storage growth. "
        f"See https://docs.feast.dev/reference/online-stores/iceberg#compaction"
    )
    self._compaction_warning_shown = True
```

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:167-168`
- `docs/reference/online-stores/iceberg.md` (documentation)

**Affected Methods:**
- `IcebergOnlineStore.online_write_batch()`

**Iceberg Operations:**
- `table.append()` - Current (append-only)
- `table.overwrite()` - Proposed (upsert by partition)
- `table.rewrite_data_files()` - Compaction
- `table.expire_snapshots()` - Cleanup

**Read-Time Deduplication:**
- Currently handled in `_convert_arrow_to_feast()` lines 591-614
- Finds latest row per entity_key by event_ts

## Acceptance Criteria

### Solution 1 (Immediate):
- [ ] Documentation added to `docs/reference/online-stores/iceberg.md`
- [ ] Warning log added on first write
- [ ] Example compaction script provided
- [ ] Compaction frequency recommendations documented

### Solution 2 (Future):
- [ ] Research Iceberg merge patterns
- [ ] Prototype overwrite-based upsert
- [ ] Benchmark write performance vs append-only
- [ ] Test concurrent write handling

## Work Log

**2026-01-16:** Issue identified during data-integrity and performance reviews

## Resources

- Data integrity review: `/home/tommyk/.claude/plans/mellow-petting-kettle.md`
- Iceberg table maintenance: https://iceberg.apache.org/docs/latest/maintenance/
- PyIceberg API: https://py.iceberg.apache.org/api/
- Redis HSET semantics: https://redis.io/commands/hset/
