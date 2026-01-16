---
status: pending
priority: p1
issue_id: "021"
tags: [error-handling, silent-failures, code-quality]
dependencies: [015]
---

# Overly Broad Exception Handling Masks Failures

## Problem Statement

Multiple locations use bare `except Exception:` or catch-all exception handling that masks critical failures like authentication errors, permission errors, and network failures. This makes debugging difficult and hides production issues.

**Why it matters:**
- **Debugging:** Hard to diagnose root cause of failures
- **Security:** Authentication failures silently ignored
- **Operations:** Production issues may go unnoticed

## Findings

**Evidence from silent-failure-hunter agent:**

### Finding 1: Generic Exception Catching (Multiple Locations)

**Location 1:** `iceberg.py:290-294` (online store)
```python
try:
    catalog.drop_table(table_identifier)
except Exception:
    # Too broad! Masks auth failures, network errors, etc.
    logger.warning(f"Failed to delete table {table_identifier}")
```

**Location 2:** `iceberg.py:360-363` (online store) - PARTIALLY FIXED
```python
try:
    catalog.create_namespace(config.namespace)
except Exception as e:
    # Better, but should catch specific NamespaceAlreadyExistsError
    if "already exists" not in str(e).lower():
        raise
```

**Location 3:** `iceberg.py:test_sql_injection_prevention_accepts_dataframes` (tests)
```python
except Exception:
    # Other exceptions are fine - we're only testing SQL injection prevention
    pass
```

### Finding 2: Multiple Bare Excepts

**Locations identified:**
- Online store cleanup: Line 290
- Namespace creation: Line 360 (improved but still broad)
- Test mocking fallbacks: Multiple test files

## Proposed Solutions

### Solution 1: Catch Specific Exceptions (Recommended)
**Pros:**
- Only handles expected failures
- Unexpected failures propagate correctly
- Better error messages

**Cons:**
- Requires knowing exception types
- More verbose

**Effort:** Small
**Risk:** Low

**Implementation:**

```python
# Fix 1: Table deletion
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError

try:
    catalog.drop_table(table_identifier)
except (NoSuchTableError, NoSuchNamespaceError):
    # Expected: table doesn't exist
    logger.debug(f"Table {table_identifier} not found (already deleted)")
except Exception as e:
    # Unexpected failures
    logger.error(f"Failed to delete table {table_identifier}: {e}", exc_info=True)
    raise

# Fix 2: Namespace creation
from pyiceberg.exceptions import NamespaceAlreadyExistsError

try:
    catalog.create_namespace(config.namespace)
except NamespaceAlreadyExistsError:
    # Expected: namespace exists
    pass
# Don't catch other exceptions - let auth/network failures propagate!
```

### Solution 2: Add Logging Before Swallowing
**Pros:**
- Maintains current behavior
- Adds visibility

**Cons:**
- Still hides failures
- Debugging still difficult

**Effort:** Trivial
**Risk:** None

**Implementation:**
```python
except Exception as e:
    logger.exception(f"Failed to delete table {table_identifier}: {e}")
    # Still swallows, but at least logged
```

## Recommended Action

Implement **Solution 1** for all production code.

Use **Solution 2** only in test code where broad catching is intentional.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py`
- `sdk/python/tests/unit/infra/online_store/test_iceberg_online_store.py`

**PyIceberg Exception Types:**
```python
from pyiceberg.exceptions import (
    NoSuchTableError,
    NoSuchNamespaceError,
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
    CommitFailedException,
    # ... etc
)
```

## Acceptance Criteria

- [ ] All production `except Exception:` replaced with specific exceptions
- [ ] Namespace creation catches only `NamespaceAlreadyExistsError`
- [ ] Table deletion catches only `NoSuchTableError` and `NoSuchNamespaceError`
- [ ] Authentication failures propagate correctly
- [ ] Test added verifying auth failures are not swallowed

## Work Log

**2026-01-16:** Issue identified during error-handling review by silent-failure-hunter agent
**2026-01-16:** Issue 015 partially addressed namespace creation, but still too broad

## Resources

- Review: Agent a08d716 findings
- Related: 015-pending-p2-exception-swallowing.md
- PyIceberg exceptions: https://py.iceberg.apache.org/api/#exceptions
