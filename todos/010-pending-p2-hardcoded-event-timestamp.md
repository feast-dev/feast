---
status: pending
priority: p2
issue_id: "010"
tags: [code-review, offline-store, correctness]
dependencies: []
---

# Hardcoded event_timestamp Column Name

## Problem Statement

The ASOF JOIN uses hardcoded `entity_df.event_timestamp` column name. Queries fail if entity DataFrame uses a different timestamp column name.

**Location:** `sdk/python/feast/infra/offline_stores/contrib/iceberg_offline_store/iceberg.py:210`

## Proposed Solutions

Make timestamp column configurable or detect from entity DataFrame schema.

## Acceptance Criteria

- [ ] Support custom timestamp column names
- [ ] Test with non-standard column names
- [ ] Backward compatible with default "event_timestamp"

## Work Log

**2026-01-16:** Identified by data-integrity-guardian agent
