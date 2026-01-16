---
status: completed
priority: p3
issue_id: "023"
tags: [code-quality, minor, cleanup]
dependencies: []
---

# Redundant Logger Import Shadows Module-Level Logger

## Problem Statement

The `online_write_batch` method imports `logging` locally and creates a new logger, shadowing the module-level logger that's already defined at the top of the file.

**Why it matters:**
- **Code Quality:** Unnecessary import
- **Consistency:** Other methods use module-level logger
- **Performance:** Minimal overhead creating duplicate logger

## Findings

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:165`

**Current Code:**
```python
# Module level (line ~30)
logger = logging.getLogger(__name__)

# Inside method (line 165)
def online_write_batch(...):
    if not hasattr(self, '_append_warning_shown'):
        import logging  # Redundant!
        logger = logging.getLogger(__name__)  # Shadows module logger!
        logger.warning(...)
```

**Evidence from code-reviewer agent:**
- Local import shadows module-level logger
- No reason for local scope logger
- Inconsistent with rest of codebase

## Proposed Solutions

### Solution 1: Use Module Logger (Recommended)
**Pros:**
- Removes redundant import
- Consistent with codebase
- Slightly faster

**Cons:**
- None

**Effort:** Trivial
**Risk:** None

**Implementation:**
```python
def online_write_batch(...):
    if not hasattr(self, '_append_warning_shown'):
        # Use module-level logger (already imported at top)
        logger.warning(
            "Iceberg online store uses append-only writes. "
            "Run periodic compaction to prevent unbounded storage growth. "
            "See https://docs.feast.dev/reference/online-stores/iceberg#compaction"
        )
        self._append_warning_shown = True
```

## Recommended Action

Remove local import, use module-level logger.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:165`

**Change:**
```diff
  def online_write_batch(...):
      if not hasattr(self, '_append_warning_shown'):
-         import logging
-         logger = logging.getLogger(__name__)
          logger.warning(...)
```

## Acceptance Criteria

- [ ] Local `import logging` removed
- [ ] Local `logger = ...` removed
- [ ] Uses module-level logger
- [ ] Warning still logged correctly
- [ ] All tests pass

## Work Log

**2026-01-16:** Issue identified during code-quality review by code-reviewer agent

## Resources

- Code review: Agent a6cc93c findings
