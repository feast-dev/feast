# Iceberg Production-Readiness Hardening Backlog

**Status**: Draft backlog + schedule

This document tracks the work needed to move the Iceberg offline/online store specs and documentation from "implemented" to "production-ready" (clear contracts, correct examples, operational guidance, and repeatable validation).

## What “production-ready” means for these specs

A spec is considered production-ready when it is:

1. **Accurate**: Matches the current implementation and test behavior.
2. **Complete**: Documents configuration, supported combinations, expected behavior, and limitations.
3. **Operable**: Provides runbooks for common failures and guidance for maintenance (compaction, retention).
4. **Verifiable**: Has a minimal, repeatable validation/benchmark plan to support stated performance and correctness claims.

## Acceptance criteria (must-have)

### Configuration contract
- [ ] Document all supported config keys with defaults for:
  - [ ] `IcebergOfflineStoreConfig` (`type: iceberg`, catalog settings, `storage_options`)
  - [ ] `IcebergOnlineStoreConfig` (`type: iceberg`, partition strategy/count, timeouts, `storage_options`)
- [ ] Provide a “required per catalog type” table (REST/SQL/Glue/Hive): which keys are required vs optional.
- [ ] State supported Feast versions and key dependency constraints (PyIceberg / DuckDB / PyArrow).

### Behavioral contract
- [ ] Offline store: point-in-time semantics, join key expectations, timestamp handling (`event_timestamp` vs `event_ts`), created timestamp usage.
- [ ] Online store: materialization semantics, “latest record” selection semantics, nullability behavior.
- [ ] Document limitations in operational terms (what breaks, what degrades, how to mitigate).

### Operations + security
- [ ] Failure modes and recovery runbook (catalog unavailable, object store failures, schema mismatch, timeouts).
- [ ] Security expectations: credentials handling, least-privilege guidance, multi-tenant namespace isolation.
- [ ] Maintenance: compaction guidance, file sizing, metadata growth, retention patterns.

### Validation
- [ ] Define a minimal CI gate (lint + Iceberg-targeted integration subset).
- [ ] Define a manual certification checklist using `examples/iceberg-local/`.
- [ ] Define a benchmark plan tied to SLO claims (p50/p95/p99, dataset scale, partition_count).

## Audit findings (current gaps)

### P0: Documentation correctness gaps
- **IcebergSource constructor mismatch**: Some docs use `IcebergSource(table=...)` but the implementation uses `IcebergSource(table_identifier=...)`.
  - Affected: `docs/reference/offline-stores/iceberg.md` (needs update), `docs/specs/iceberg_quickstart.md` (fixed).
- **Offline store type string mismatch**: Some docs show a fully-qualified class path for `offline_store.type`, but the implementation expects `type: iceberg`.
  - Affected: `docs/reference/offline-stores/iceberg.md` (needs update), `docs/specs/iceberg_quickstart.md` (fixed).
- **Online spec contradictions**: `docs/specs/iceberg_online_store.md` contained an outdated “Phase 3 not started” checklist despite being marked complete.
  - Fixed: replaced the outdated section with a “Known Limitations / Hardening Backlog” pointer.

### P0: Spec claims that need code verification
- **Online column projection**: the spec and reference docs describe column projection, but the current implementation builds a column list without applying projection in the scan.
- **Online partition pruning story**: the current design needs a careful audit to ensure the partition strategy and row filters actually result in metadata pruning (avoid double-bucketing or filtering on non-partition expressions).

### P1: Missing production “operability” content
- No clear runbooks for catalog/object-store failures.
- No compaction/retention guidance beyond high-level mentions.
- No supported/certified matrix (what configurations are actually validated).

## Prioritized backlog + schedule

### P0 (Day 0–2): Make docs/specs accurate and internally consistent
- [ ] Update `docs/reference/offline-stores/iceberg.md` examples to:
  - [ ] use `offline_store.type: iceberg`
  - [ ] use `IcebergSource(table_identifier=...)`
- [ ] Ensure `docs/specs/iceberg_quickstart.md` matches current code (offline store type + IcebergSource args) (done).
- [ ] Ensure `docs/specs/iceberg_online_store.md` does not contain contradictory status sections (done).
- [ ] Add a single “Supported / Not supported” callout to each spec (offline + online).

### P0 (Day 0–2): Close spec/impl gaps that affect correctness claims
- [ ] Online store: apply real column projection in reads (so requested features don’t require full table scans).
- [ ] Online store: validate partition strategy and pruning (ensure row filters align with partition spec and avoid double bucketing).
- [ ] Online store: fix mutable default config (`storage_options`) to use a default factory.

### P1 (Week 1): Operability + security hardening in docs/specs
- [ ] Add “Failure modes & runbook” sections:
  - [ ] Offline store spec: scan failures, MOR/COW behavior, join failures, bad schemas.
  - [ ] Online store spec: table missing, catalog unavailable, object-store transient failures, timeouts.
- [ ] Add “Maintenance” sections:
  - [ ] Compaction guidance (what to run, why, and consequences if not).
  - [ ] File sizing and metadata growth guidance.
  - [ ] Retention patterns (since TTL is not supported at retrieval).
- [ ] Add “Security & multi-tenancy” sections:
  - [ ] `storage_options` secret handling
  - [ ] least-privilege IAM patterns
  - [ ] namespace isolation patterns

### P2 (Weeks 2–3): Validation gates + benchmarking
- [ ] Define a minimal CI gate for Iceberg (lint + targeted integration tests).
- [ ] Add a manual certification checklist using `examples/iceberg-local/`.
- [ ] Add a benchmark harness plan (and optionally initial benchmark results) tied to spec latency targets.

## Proposed “certified matrix” (initial)

Initial certification targets (expand over time):

- **Certified (initial)**:
  - SQL catalog + local filesystem warehouse
  - REST catalog + S3-compatible warehouse (MinIO for development; AWS S3 for production)
- **Documented (not yet certified)**:
  - AWS Glue catalog + S3
  - Hive catalog
  - Cloudflare R2 Data Catalog (Beta)


## Online-store performance target ("good enough")

Until benchmarks exist, the docs should treat online performance as a **target range** rather than a guarantee.

- **Target (warm metadata, entity_hash, partition_count=256, batch <= 100, <= 20 feature columns)**:
  - p50 <= 75ms
  - p95 <= 200ms

Benchmarks in P2 should validate this target (and tighten or relax it with evidence).
