---
status: completed
priority: p1
issue_id: "020"
tags: [security, validation, data-integrity]
dependencies: [003]
resolved_date: "2026-01-17"
resolution: "Added TTL bounds validation (1 second to 365 days) and math.isfinite() check. 3 comprehensive tests passing."
---

# Missing TTL Value Validation

## Problem Statement

The TTL filtering implementation (fix 003) does not validate that `fv.ttl.total_seconds()` produces a safe value before interpolating into SQL. Negative or extremely large TTL values could cause query errors or unexpected behavior.

**Why it matters:**
- **Security:** Potential for SQL errors revealing system info
- **Correctness:** Negative TTLs would invert time logic
- **Robustness:** Large TTLs could cause numeric overflow

## Findings

**Location:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:221-227`

**Current Code:**
```python
if fv.ttl and fv.ttl.total_seconds() > 0:
    ttl_seconds = fv.ttl.total_seconds()
    query += (
        f" AND {fv.name}.{fv.batch_source.timestamp_field} >= "
        f"entity_df.event_timestamp - INTERVAL '{ttl_seconds}' SECOND"
    )
```

**Missing Validations:**
- No check that `ttl_seconds` is finite
- No upper bound check
- No check for very small values (< 1 second)
- Float value directly interpolated into SQL

**Potential Attack:**
```python
# Malicious TTL
fv.ttl = timedelta(seconds=float('inf'))

# Resulting SQL
"... - INTERVAL 'inf' SECOND"  # SQL error!
```

**Evidence from security-sentinel agent:**
- TTL value goes directly into SQL string
- No sanitization or bounds checking
- Could reveal DuckDB version info via error messages

## Proposed Solutions

### Solution 1: Add Value Validation (Recommended)
**Pros:**
- Simple bounds check
- Prevents edge cases
- Provides clear error messages

**Cons:**
- Adds minimal overhead

**Effort:** Trivial
**Risk:** None

**Implementation:**
```python
if fv.ttl and fv.ttl.total_seconds() > 0:
    ttl_seconds = fv.ttl.total_seconds()

    # Validate TTL bounds
    if not (1 <= ttl_seconds <= 31536000):  # 1 second to 1 year
        raise ValueError(
            f"Feature view '{fv.name}' has invalid TTL: {fv.ttl}. "
            f"TTL must be between 1 second and 365 days."
        )

    if not isinstance(ttl_seconds, (int, float)) or not math.isfinite(ttl_seconds):
        raise ValueError(
            f"Feature view '{fv.name}' has non-finite TTL: {ttl_seconds}"
        )

    # Safe to interpolate
    query += (
        f" AND {fv.name}.{fv.batch_source.timestamp_field} >= "
        f"entity_df.event_timestamp - INTERVAL '{ttl_seconds}' SECOND"
    )
```

### Solution 2: Use DuckDB INTERVAL Literals
**Pros:**
- More type-safe
- Better SQL readability

**Cons:**
- Requires timedelta-to-interval conversion
- More complex

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
def timedelta_to_interval(td: timedelta) -> str:
    """Convert timedelta to DuckDB INTERVAL literal."""
    days = td.days
    seconds = td.seconds

    if days > 0:
        return f"INTERVAL '{days}' DAY + INTERVAL '{seconds}' SECOND"
    else:
        return f"INTERVAL '{seconds}' SECOND"

interval_expr = timedelta_to_interval(fv.ttl)
query += f" AND ... >= entity_df.event_timestamp - {interval_expr}"
```

## Recommended Action

Implement **Solution 1** immediately to close security gap.

Validate TTL values before SQL interpolation.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:221-227`

**Reasonable TTL Bounds:**
- Minimum: 1 second (prevents sub-second TTLs)
- Maximum: 31536000 seconds (365 days)
- Alternative max: 86400 * 30 (30 days) for stricter control

**DuckDB INTERVAL Syntax:**
https://duckdb.org/docs/sql/data_types/interval

## Acceptance Criteria

- [ ] TTL value validated before SQL interpolation
- [ ] Bounds check enforces reasonable range
- [ ] Non-finite values (inf, nan) rejected
- [ ] Clear error messages for invalid TTLs
- [ ] Test added for edge cases (negative, inf, zero, very large)

## Work Log

**2026-01-16:** Issue identified during security review by security-sentinel agent

## Resources

- Security review: Agent ac166e1 findings
- Related: 003-pending-p1-missing-ttl-filtering.md
- Python math.isfinite: https://docs.python.org/3/library/math.html#math.isfinite
