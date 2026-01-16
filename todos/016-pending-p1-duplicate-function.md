---
status: pending
priority: p2
issue_id: "016"
tags: [code-quality, duplication, maintainability]
dependencies: []
---

# Duplicate _arrow_to_iceberg_type Function

## Problem Statement

The `_arrow_to_iceberg_type` function is duplicated in the online store implementation at lines 521-539 and 690-706. This violates DRY principles and creates maintenance burden.

**Why it matters:**
- **Maintainability:** Bug fixes must be applied in two places
- **Code Quality:** Increases codebase size unnecessarily
- **Risk:** Implementations may drift over time

## Findings

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py`

**Duplicate Implementations:**
- Lines 521-539: First occurrence
- Lines 690-706: Exact duplicate

**Evidence from code-reviewer agent:**
- Both functions are byte-for-byte identical
- No variation in behavior or documentation
- Clear refactoring opportunity

## Proposed Solutions

### Solution 1: Keep Single Instance (Recommended)
**Pros:**
- Simple deletion
- Reduces code by ~18 lines
- No new complexity

**Cons:**
- None

**Effort:** Trivial
**Risk:** None

**Implementation:**
```python
# Delete lines 690-706 (second occurrence)
# Keep lines 521-539 as the canonical version
```

### Solution 2: Extract to Shared Module
**Pros:**
- Could be shared with offline store if needed
- More discoverable location

**Cons:**
- Overkill for single-file use
- Violates YAGNI

**Effort:** Small
**Risk:** Low

## Recommended Action

**Solution 1**: Delete the duplicate at lines 690-706.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:690-706`

**Function Signature:**
```python
def _arrow_to_iceberg_type(arrow_type: pyarrow.DataType) -> IcebergType:
    """Convert PyArrow type to Iceberg type."""
```

## Acceptance Criteria

- [ ] Duplicate function removed (lines 690-706)
- [ ] All tests still pass
- [ ] No references to deleted function location

## Work Log

**2026-01-16:** Issue identified during code-quality review by code-reviewer agent

## Resources

- Code review: Agent a6cc93c findings
