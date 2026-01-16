---
status: pending
priority: p2
issue_id: "015"
tags: [code-review, error-handling, online-store]
dependencies: []
---

# Exception Swallowing in Namespace Creation

## Problem Statement

Bare `except Exception` swallows all errors including permission and network failures when creating namespaces.

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:360-363`

**Current:**
```python
try:
    catalog.create_namespace(config.namespace)
except Exception:
    pass  # Namespace already exists
```

## Proposed Solutions

### Solution 1: Catch Specific Exception
```python
from pyiceberg.exceptions import NamespaceAlreadyExistsError

try:
    catalog.create_namespace(config.namespace)
except NamespaceAlreadyExistsError:
    pass  # Expected, namespace exists
# Let other exceptions propagate
```

## Acceptance Criteria

- [ ] Only catch NamespaceAlreadyExistsError
- [ ] Permission errors propagate properly
- [ ] Test verifies error handling

## Work Log

**2026-01-16:** Identified by security-sentinel agent
