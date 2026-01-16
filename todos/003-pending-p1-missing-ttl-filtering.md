---
status: resolved
priority: p1
issue_id: "003"
tags: [code-review, data-integrity, offline-store, ml-correctness]
dependencies: []
resolution: fixed
fixed_in_commit: HEAD
---

# Missing TTL Filtering in Point-in-Time Joins

## Problem Statement

The ASOF JOIN query construction in `get_historical_features()` does not respect the FeatureView's `ttl` (time-to-live) configuration. This allows arbitrarily stale features to be included in training datasets, violating the intended freshness constraints and potentially causing data leakage in ML models.

**Why it matters:**
- **Data Leakage:** Stale features may contain information that wouldn't be available at prediction time
- **Model Performance:** Models trained on stale data perform poorly in production
- **Correctness:** Violates Feast's core guarantee of point-in-time correctness

## Findings

**Location:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:200-211`

**Current Implementation:**
```python
query += f" ASOF LEFT JOIN {fv.name} ON "
join_conds = [f"entity_df.{k} = {fv.name}.{k}" for k in fv.join_keys]
query += " AND ".join(join_conds)
query += f" AND entity_df.event_timestamp >= {fv.name}.{fv.batch_source.timestamp_field}"
```

**Missing Logic:**
- No upper bound on how old a feature can be
- FeatureView `ttl` attribute completely ignored
- Features from weeks/months/years ago could be joined

**Evidence from data-integrity-guardian agent:**
- All other Feast offline stores (Spark, Snowflake, BigQuery) implement TTL filtering
- TTL is a critical component of point-in-time correctness
- Without TTL, features outside their validity window are included

**Example Scenario:**
```python
# Feature view with 24-hour TTL
customer_features = FeatureView(
    name="customer_features",
    ttl=timedelta(hours=24),  # Features only valid for 24 hours
    ...
)

# Current behavior: If last feature update was 3 days ago, it's still joined
# Expected behavior: Features older than 24 hours should be NULL
```

## Proposed Solutions

### Solution 1: Add TTL Filter to ASOF JOIN (Recommended)
**Pros:**
- Aligns with all other Feast offline stores
- Prevents data leakage
- Simple to implement

**Cons:**
- May reduce feature availability if data is stale
- Could break existing workflows expecting stale data

**Effort:** Small
**Risk:** Low

**Implementation:**
```python
query += f" ASOF LEFT JOIN {fv.name} ON "
join_conds = [f"entity_df.{k} = {fv.name}.{k}" for k in fv.join_keys]
query += " AND ".join(join_conds)

# Lower bound: feature timestamp <= entity timestamp
query += f" AND entity_df.event_timestamp >= {fv.name}.{fv.batch_source.timestamp_field}"

# Upper bound: feature timestamp >= entity timestamp - TTL
if fv.ttl and fv.ttl.total_seconds() > 0:
    ttl_seconds = fv.ttl.total_seconds()
    query += (
        f" AND {fv.name}.{fv.batch_source.timestamp_field} >= "
        f"entity_df.event_timestamp - INTERVAL '{ttl_seconds}' SECOND"
    )
```

### Solution 2: Make TTL Optional via Config
**Pros:**
- Backward compatible
- Allows users to opt-in

**Cons:**
- Violates Feast semantics
- Inconsistent with other stores
- Users may not know to enable it

**Effort:** Small
**Risk:** Medium (semantic confusion)

**Implementation:**
```python
# Not recommended - violates Feast guarantees
if config.offline_store.enforce_ttl:  # New config option
    if fv.ttl and fv.ttl.total_seconds() > 0:
        # Add TTL filter
```

## Recommended Action

Implement **Solution 1** immediately. This is a correctness issue that violates Feast's core guarantees.

Add integration test verifying TTL is enforced correctly.

## Technical Details

**Affected Files:**
- `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:200-211`

**Affected Methods:**
- `IcebergOfflineStore.get_historical_features()`

**Related FeatureView Attributes:**
- `FeatureView.ttl` (type: `timedelta`)

**DuckDB Interval Syntax:**
- `INTERVAL 'n' SECOND/MINUTE/HOUR/DAY`
- Total seconds conversion: `ttl.total_seconds()`

**Comparison with Other Stores:**
- Spark: Implements TTL filtering in Spark SQL
- Snowflake: Implements TTL filtering in Snowflake SQL
- BigQuery: Implements TTL filtering in BigQuery SQL

## Acceptance Criteria

- [x] TTL filter added to ASOF JOIN when `fv.ttl` is defined
- [x] TTL filter uses correct DuckDB INTERVAL syntax
- [ ] Integration test added with:
  - Feature view with 1-hour TTL
  - Entity timestamp at T
  - Features at T-30min (should join), T-2hours (should be NULL)
- [ ] Verify behavior matches Snowflake/Spark stores
- [ ] Documentation updated explaining TTL enforcement

## Work Log

**2026-01-16:** Issue identified during data-integrity review by data-integrity-guardian agent
**2026-01-16:** FIXED - Added correct TTL filtering (lines 221-227). Original plan had BACKWARDS inequality which would have broken point-in-time correctness. Kieran's review caught this critical bug.

## Resources

- Data integrity review: `/home/tommyk/.claude/plans/mellow-petting-kettle.md`
- FeatureView TTL docs: https://docs.feast.dev/reference/feature-views
- Spark offline store TTL implementation: `sdk/python/feast/infra/offline_stores/spark.py`
- DuckDB interval syntax: https://duckdb.org/docs/sql/data_types/interval
