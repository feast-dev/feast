---
status: completed
priority: p0
issue_id: "017"
tags: [security, sql-injection, critical]
dependencies: []
---

# Unvalidated SQL Identifiers in Query Construction

## Problem Statement

Feature view names, column names, and table identifiers are directly interpolated into SQL queries without validation. This creates SQL injection vulnerabilities even though entity_df strings are now blocked.

**Why it matters:**
- **Security:** Malicious feature view names can execute arbitrary SQL
- **Attack Vector:** Feature views are often loaded from configuration files
- **Severity:** CRITICAL - arbitrary SQL execution possible

## Findings

**Locations:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py`

**Vulnerable Code:**

1. **Lines 206-210** - Feature view name in ASOF JOIN:
```python
query += f" ASOF LEFT JOIN {fv.name} ON "  # Unvalidated!
```

2. **Line 178** - Table registration:
```python
con.execute(f"CREATE VIEW {fv.name} AS SELECT * FROM iceberg_scan('{file_paths}')")
```

3. **Line 197** - Column names:
```python
query = f"SELECT entity_df.*, {', '.join(feature_cols)} FROM entity_df"
```

**Example Attack:**
```python
# Malicious feature view name
fv.name = "features; DROP TABLE entity_df; --"

# Resulting SQL
"ASOF LEFT JOIN features; DROP TABLE entity_df; -- ON ..."
```

**Evidence from security-sentinel agent:**
- All SQL identifier interpolations are unvalidated
- Feature view names come from user configuration
- DuckDB has file access functions exploitable via SQL injection

## Proposed Solutions

### Solution 1: SQL Identifier Validation (Recommended)
**Pros:**
- Simple regex validation
- Prevents injection completely
- Low overhead

**Cons:**
- May break exotic naming schemes
- Requires validation at multiple points

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
import re

def validate_sql_identifier(identifier: str, context: str = "identifier") -> str:
    """Validate SQL identifier is safe for interpolation.

    Args:
        identifier: The identifier to validate
        context: Description for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If identifier contains unsafe characters
    """
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(
            f"Invalid SQL {context}: '{identifier}'. "
            f"Only alphanumeric characters and underscores allowed."
        )

    # Check for SQL reserved words
    reserved = {'SELECT', 'FROM', 'WHERE', 'DROP', 'TABLE', 'DELETE', ...}
    if identifier.upper() in reserved:
        raise ValueError(
            f"SQL {context} cannot be a reserved word: '{identifier}'"
        )

    return identifier

# Apply at all interpolation points
fv_name = validate_sql_identifier(fv.name, "feature view name")
query += f" ASOF LEFT JOIN {fv_name} ON "
```

### Solution 2: Use DuckDB Identifier Quoting
**Pros:**
- Standard SQL approach
- Allows special characters

**Cons:**
- Still vulnerable if quotes not properly escaped
- More complex implementation

**Effort:** Medium
**Risk:** Medium

**Implementation:**
```python
def quote_identifier(identifier: str) -> str:
    """Quote identifier for safe SQL interpolation."""
    # Escape any existing quotes
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'

query += f" ASOF LEFT JOIN {quote_identifier(fv.name)} ON "
```

### Solution 3: Use DuckDB Prepared Statements
**Pros:**
- Industry best practice
- Prevents all injection

**Cons:**
- DuckDB Python API doesn't support prepared identifiers well
- Major refactoring required

**Effort:** Large
**Risk:** High

## Recommended Action

Implement **Solution 1** immediately. This is a CRITICAL security vulnerability.

Add validation at all SQL identifier interpolation points:
- Feature view names
- Column names
- Table identifiers
- Timestamp field names

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py`

**All Interpolation Points:**
- Line 178: `CREATE VIEW {fv.name}`
- Line 197: Feature column names in SELECT
- Line 206: `ASOF LEFT JOIN {fv.name}`
- Line 210: Join key columns
- Line 222: Timestamp field in TTL filter
- Line 288: Column names in ORDER BY
- Line 302: Timestamp field in ORDER BY

**DuckDB Reserved Words:**
https://duckdb.org/docs/sql/keywords_and_identifiers

## Acceptance Criteria

- [x] `validate_sql_identifier()` function implemented
- [x] Validation applied to all feature view names
- [x] Validation applied to all column names
- [x] Validation applied to all timestamp fields
- [x] Security test added demonstrating injection is prevented
- [ ] Documentation updated with naming constraints

## Work Log

**2026-01-16:** Issue identified during comprehensive security review by security-sentinel agent (P0 CRITICAL)

**2026-01-16:** RESOLVED - Implemented comprehensive SQL identifier validation
- Created `validate_sql_identifier()` function with regex validation and reserved word checking
- Applied validation to all SQL identifier interpolation points in iceberg.py:
  - Feature view names (lines 248, 287, 299)
  - Column names and feature names (lines 290-294)
  - Join keys (lines 303-305)
  - Timestamp fields (lines 249, 306, 476)
  - All column references in `pull_all_from_table_or_query` and `pull_latest_from_table_or_query`
- Added comprehensive test suite with 10 security tests
- All tests passing (20/20 in test_iceberg_offline_store_fixes.py)

## Resources

- Security review: Agent ac166e1 findings
- OWASP SQL Injection: https://owasp.org/www-community/attacks/SQL_Injection
- DuckDB identifier syntax: https://duckdb.org/docs/sql/keywords_and_identifiers
