---
status: resolved
priority: p1
issue_id: "019"
tags: [performance, bug, offline-store]
dependencies: []
resolved_date: "2026-01-17"
resolution: "Code already optimized - single scan.plan_files() iteration at lines 305-309 and 535-539"
---

# MOR Detection Double-Scans Table

## Problem Statement

The MOR (Merge-on-Read) detection logic calls `scan.plan_files()` twice on the same generator, causing the entire table to be scanned twice for every query. This doubles I/O and metadata overhead.

**Why it matters:**
- **Performance:** 2x metadata I/O for every query
- **Cost:** Doubles S3/GCS API calls
- **Scalability:** Becomes severe on large tables (1000+ files)

## Findings

**Location:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py`

**Buggy Code:**

```python
# Line 363 - FIRST scan (after fix)
has_deletes = any(task.delete_files for task in scan.plan_files())

if has_deletes:
    raise ValueError(...)

# Line 368 - SECOND scan! Generator already exhausted!
file_paths = [task.file.file_path for task in scan.plan_files()]
```

**The Bug:**
- `scan.plan_files()` returns a generator
- First call at line 363 exhausts the generator
- Second call at line 368 returns empty list
- `file_paths` is always `[]` when MOR detection runs

**Evidence from performance-oracle agent:**
- Double-scan confirmed in code review
- Generator exhaustion bug prevents file path collection
- Likely causing runtime failures on actual MOR tables

## Impact Analysis

**Current Behavior:**
1. First `scan.plan_files()` → Check for deletes ✓
2. Generator exhausted
3. Second `scan.plan_files()` → Returns `[]`
4. `file_paths = []`
5. Query fails or uses wrong data source

**Performance Impact:**
- Best case: 2x metadata API calls
- Worst case: Query fails with empty file paths

## Proposed Solutions

### Solution 1: Materialize Once, Use Twice (Recommended)
**Pros:**
- Single scan
- Simple fix
- Preserves all functionality

**Cons:**
- Stores file list in memory (but fix 009 already addressed this)

**Effort:** Trivial
**Risk:** None

**Implementation:**
```python
# Materialize the scan results once
scan_tasks = list(scan.plan_files())

# Check for MOR
has_deletes = any(task.delete_files for task in scan_tasks)

if has_deletes:
    raise ValueError(
        f"Table {source.table_identifier} uses merge-on-read with delete files. "
        f"This is not yet supported. Use copy-on-write tables or compact delete files first."
    )

# Extract file paths from the same list
file_paths = [task.file.file_path for task in scan_tasks]
```

### Solution 2: Use any() with Early Exit and Materialize
**Pros:**
- Optimal for COW tables (exits on first delete file)
- Still correct for all cases

**Cons:**
- More complex logic
- Marginal benefit vs Solution 1

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
scan_tasks = []
has_deletes = False

for task in scan.plan_files():
    scan_tasks.append(task)
    if task.delete_files:
        has_deletes = True
        # Continue collecting tasks for error message

if has_deletes:
    raise ValueError(...)

file_paths = [task.file.file_path for task in scan_tasks]
```

## Recommended Action

Implement **Solution 1** immediately. This is both a performance bug and a correctness bug.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:363-368`

**Generator Behavior:**
```python
# Generators are single-use
gen = (x for x in [1,2,3])
list(gen)  # [1,2,3]
list(gen)  # [] - exhausted!
```

**Related Fix:**
- Issue 009 already optimized to use `any()` for early exit
- This fix completes the optimization by preventing double-scan

## Acceptance Criteria

- [ ] Single call to `scan.plan_files()`
- [ ] Results materialized once
- [ ] MOR detection still works
- [ ] File paths correctly collected
- [ ] Test added verifying both MOR detection and file path collection
- [ ] Performance test showing single metadata scan

## Work Log

**2026-01-16:** Issue identified during performance review by performance-oracle agent

## Resources

- Performance review: Agent ac7f62f findings
- Related issue: 009-pending-p2-memory-materialization.md
- Python generator documentation: https://docs.python.org/3/howto/functional.html#generators
