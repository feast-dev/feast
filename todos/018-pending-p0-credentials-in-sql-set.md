---
status: completed
priority: p0
issue_id: "018"
tags: [security, credentials, exposure, critical]
dependencies: []
resolved_date: "2026-01-16"
---

# Credentials Exposed in SQL SET Commands

## Problem Statement

The offline store uses DuckDB's SQL `SET` commands to configure S3 credentials, which are visible in query logs, error messages, and DuckDB's internal query history. This exposes AWS credentials.

**Why it matters:**
- **Security:** AWS credentials logged in plaintext
- **Compliance:** Violates secret management best practices
- **Attack Vector:** Credentials accessible via DuckDB query history
- **Severity:** CRITICAL - credential exposure

## Findings

**Location:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:150-155`

**Vulnerable Code:**
```python
# Configure DuckDB with storage options from config
storage_opts = config.offline_store.storage_options or {}
for key, value in storage_opts.items():
    # CRITICAL: Credentials visible in SQL!
    con.execute(f"SET {key} = '{value}'")
```

**Example Exposure:**
```python
storage_options = {
    "s3.access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "s3.secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
}

# Resulting SQL (visible in logs!)
# SET s3.access_key_id = 'AKIAIOSFODNN7EXAMPLE'
# SET s3.secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
```

**Evidence from security-sentinel agent:**
- Credentials appear in:
  - DuckDB query logs
  - Exception stack traces
  - Query history tables
  - Debug output
- No redaction or masking applied
- Violates AWS security best practices

## Proposed Solutions

### Solution 1: Use DuckDB Python API (Recommended)
**Pros:**
- Credentials never enter SQL strings
- No exposure in logs
- Industry best practice

**Cons:**
- Requires DuckDB API research
- May need version check

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
import duckdb

con = duckdb.connect()

# Use Python API instead of SQL SET
storage_opts = config.offline_store.storage_options or {}
for key, value in storage_opts.items():
    # Credentials stay in Python, never in SQL
    if key.startswith("s3."):
        duckdb_key = key.replace("s3.", "s3_")
        con.execute("SET ?=?", [duckdb_key, value])  # Parameterized
```

**Note:** Need to verify DuckDB supports parameterized SET statements.

### Solution 2: Use DuckDB Configuration Dict
**Pros:**
- Clean API
- No SQL strings
- Built-in support

**Cons:**
- May not support all options
- Requires DuckDB >= 0.8.0

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
# Pass config at connection time
con = duckdb.connect(config={
    's3_access_key_id': storage_opts.get('s3.access_key_id'),
    's3_secret_access_key': storage_opts.get('s3.secret_access_key'),
    's3_region': storage_opts.get('s3.region'),
    ...
})
```

### Solution 3: Use Environment Variables
**Pros:**
- Standard AWS credential chain
- No code changes needed
- Works with IAM roles

**Cons:**
- Less flexible for multi-tenant scenarios
- Requires documentation update

**Effort:** Trivial
**Risk:** None

**Implementation:**
```python
# Document that users should use AWS env vars instead:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# Remove storage_options entirely
# DuckDB will auto-detect from environment
```

## Recommended Action

Implement **Solution 2** (DuckDB config dict) with **Solution 3** (env vars) as fallback.

Remove all SQL SET commands for credentials.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:150-155`

**DuckDB Configuration:**
https://duckdb.org/docs/configuration/overview

**AWS Credential Best Practices:**
https://docs.aws.amazon.com/general/latest/gr/aws-access-keys-best-practices.html

## Acceptance Criteria

- [ ] No credentials in SQL SET commands
- [ ] Use DuckDB Python config API
- [ ] Verify credentials not in query logs
- [ ] Security test verifying no credential exposure
- [ ] Documentation updated with credential configuration
- [ ] Support for AWS environment variables

## Work Log

**2026-01-16:** Issue identified during comprehensive security review by security-sentinel agent (P0 CRITICAL)

## Resources

- Security review: Agent ac166e1 findings
- DuckDB S3 configuration: https://duckdb.org/docs/extensions/httpfs
- AWS credential provider chain: https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html
