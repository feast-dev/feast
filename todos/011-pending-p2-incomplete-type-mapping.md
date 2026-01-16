---
status: pending
priority: p2
issue_id: "011"
tags: [code-review, type-mapping, offline-store, online-store]
dependencies: []
---

# Incomplete Type Mapping for Complex Types

## Problem Statement

`iceberg_to_feast_value_type()` returns `UNKNOWN` for list, map, struct, and decimal types. Features using these types will fail or be silently dropped.

**Location:** `sdk/python/feast/type_map.py:1236-1252`

## Proposed Solutions

### Solution 1: Add List Type Support
```python
def iceberg_to_feast_value_type(iceberg_type_as_str: str) -> ValueType:
    # Parse list<T> types
    if iceberg_type_as_str.startswith("list<"):
        inner_type = iceberg_type_as_str[5:-1]  # Extract T from list<T>
        if inner_type == "string":
            return ValueType.STRING_LIST
        elif inner_type == "int":
            return ValueType.INT32_LIST
        elif inner_type == "long":
            return ValueType.INT64_LIST
        elif inner_type == "double":
            return ValueType.DOUBLE_LIST
        elif inner_type == "boolean":
            return ValueType.BOOL_LIST
        # Add more as needed
    
    # Existing scalar type mapping...
```

## Acceptance Criteria

- [ ] Support list<T> for common types
- [ ] Document unsupported types (map, struct)
- [ ] Raise clear error for unsupported types
- [ ] Integration test for list types

## Work Log

**2026-01-16:** Identified by data-integrity-guardian agent
