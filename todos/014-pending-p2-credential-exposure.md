---
status: completed
priority: p2
issue_id: "014"
tags: [code-review, security, logging]
dependencies: []
resolved_date: "2026-01-17"
resolution: "Credentials now passed via DuckDB parameterized queries ($1 placeholder), never interpolated into SQL strings. _configure_duckdb_httpfs() uses con.execute(sql, [credential]) pattern."
test_coverage: "TestCredentialSecurityFixes: 6/6 tests passing (test_credentials_not_in_sql_strings, test_credentials_use_parameterized_queries, test_no_credential_exposure_in_error_messages)"
---

# Credential Exposure Risk in Logging

## Problem Statement

Exception messages may contain connection strings with credentials. The `storage_options` dictionary is passed around without redaction.

**Location:** `sdk/python/feast/infra/online_stores/contrib/iceberg_online_store/iceberg.py:281-283`

## Proposed Solutions

### Solution 1: Sanitize Exception Messages
```python
def _sanitize_error(error_msg: str, config: IcebergOnlineStoreConfig) -> str:
    """Redact credentials from error messages."""
    sanitized = error_msg
    for key in ['s3.access-key-id', 's3.secret-access-key', 's3.session-token']:
        if key in config.storage_options:
            sanitized = sanitized.replace(config.storage_options[key], '***REDACTED***')
    return sanitized

# Usage:
except Exception as e:
    sanitized_msg = _sanitize_error(str(e), config)
    logger.warning(f"Failed to delete table {table_identifier}: {sanitized_msg}")
```

## Acceptance Criteria

- [ ] Exception sanitization implemented
- [ ] storage_options never logged in full
- [ ] Test verifies credentials not in logs

## Work Log

**2026-01-16:** Identified by security-sentinel agent
