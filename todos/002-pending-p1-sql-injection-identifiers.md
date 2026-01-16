---
status: pending
priority: p1
issue_id: "002"
tags: [code-review, security, sql-injection, offline-store]
dependencies: []
---

# SQL Injection in Feature View and Column Names

## Problem Statement

Feature view names, feature names, and column names are directly interpolated into SQL queries without validation or sanitization. Malicious names could inject SQL commands into DuckDB queries.

**Why it matters:** Users who can define feature views or data sources could inject SQL to access unauthorized data, cause denial of service, or execute arbitrary commands through DuckDB.

## Findings

**Locations:**
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:178`
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:197`
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:206-210`
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:238-239`

**Vulnerable Code Examples:**
```python
# Line 178: Feature view name injection
con.execute(f"CREATE VIEW {fv.name} AS SELECT * FROM read_parquet({file_paths})")

# Line 197: Feature name injection
query += f", {fv.name}.{feature.name} AS {feature_name}"

# Line 206-210: Join conditions with unvalidated names
query += f" ASOF LEFT JOIN {fv.name} ON "
join_conds = [f"entity_df.{k} = {fv.name}.{k}" for k in fv.join_keys]
```

**Severity:** CRITICAL - SQL injection through identifier interpolation

**Exploitability:** Medium - Requires ability to define feature views with malicious names

## Proposed Solutions

### Solution 1: Strict Identifier Validation (Recommended)
**Pros:**
- Simple and effective
- Minimal performance overhead
- Clear error messages

**Cons:**
- May break existing feature views with special characters
- Requires migration for non-compliant names

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
import re

def validate_sql_identifier(identifier: str, context: str = "identifier") -> str:
    """
    Validate that an identifier is safe for SQL interpolation.

    Args:
        identifier: The identifier to validate
        context: Description for error messages (e.g., "feature view name")

    Returns:
        The validated identifier

    Raises:
        ValueError: If identifier contains unsafe characters
    """
    if not identifier:
        raise ValueError(f"{context} cannot be empty")

    # Allow only: letters, numbers, underscores, starting with letter or underscore
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(
            f"{context} '{identifier}' contains invalid characters. "
            f"Only alphanumeric characters and underscores are allowed, "
            f"and must start with a letter or underscore."
        )

    # Prevent SQL keywords (optional but recommended)
    SQL_KEYWORDS = {'SELECT', 'FROM', 'WHERE', 'DROP', 'DELETE', 'UPDATE', 'INSERT'}
    if identifier.upper() in SQL_KEYWORDS:
        raise ValueError(f"{context} '{identifier}' is a reserved SQL keyword")

    return identifier

# Apply to all identifiers:
validate_sql_identifier(fv.name, "feature view name")
validate_sql_identifier(feature.name, "feature name")
for key in fv.join_keys:
    validate_sql_identifier(key, "join key")
```

### Solution 2: DuckDB Identifier Quoting
**Pros:**
- Allows special characters in names
- Standard SQL approach

**Cons:**
- Still vulnerable if quoting is improperly done
- More complex to implement correctly

**Effort:** Medium
**Risk:** Medium

**Implementation:**
```python
def quote_identifier(identifier: str) -> str:
    """Quote an identifier for safe SQL interpolation."""
    # Escape any double quotes in the identifier
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'

# Usage:
con.execute(f"CREATE VIEW {quote_identifier(fv.name)} AS ...")
```

### Solution 3: Use DuckDB Parameter Binding (Not Feasible)
**Pros:**
- Industry best practice

**Cons:**
- DuckDB doesn't support parameterized identifiers
- Not applicable for DDL statements

**Effort:** N/A
**Risk:** N/A

## Recommended Action

Implement **Solution 1** (strict validation) across all identifier usage in the offline store.

Create a shared `validate_sql_identifier()` function and apply it consistently.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py`

**Affected Methods:**
- `get_historical_features()`
- `pull_latest_from_table_or_query()`
- `pull_all_from_table_or_query()`
- `_setup_duckdb_source()`

**New Utility Module:**
- Consider creating `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/sql_utils.py`

## Acceptance Criteria

- [ ] `validate_sql_identifier()` function implemented and tested
- [ ] All feature view names validated before SQL interpolation
- [ ] All feature names validated before SQL interpolation
- [ ] All column names validated before SQL interpolation
- [ ] Security test added demonstrating injection prevention
- [ ] Error messages guide users to fix invalid names
- [ ] Documentation updated with naming constraints

## Work Log

**2026-01-16:** Issue identified during security review by security-sentinel agent

## Resources

- Security review: `/home/tommyk/.claude/plans/mellow-petting-kettle.md`
- Python regex for identifiers: https://docs.python.org/3/reference/lexical_analysis.html#identifiers
- SQL identifier rules: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
