# RFC: Feature View Versioning

**Status:** In Review
**Authors:** @farceo
**Branch:** `featureview-versioning`
**Date:** 2026-03-17

## Summary

This RFC proposes adding automatic version tracking to Feast feature views. Every time `feast apply` detects a schema or UDF change to a feature view, a versioned snapshot is saved to the registry. Users can list version history, pin serving to a prior version, and optionally query specific versions at read time using `@v<N>` syntax.

## Motivation

Today, when a feature view's schema changes, the old definition is silently overwritten. This creates several problems:

1. **No audit trail.** Teams can't answer "what did this feature view look like last week?" or "who changed the schema and when?"
2. **No safe rollback.** If a schema change breaks a downstream model, there's no way to revert to the previous definition without manually reconstructing it.
3. **No multi-version serving.** During migrations, teams often need to serve both the old and new schema simultaneously (e.g., model A uses v1 features, model B uses v2 features). This is currently impossible without creating entirely separate feature views.

## Diagrams

### Lifecycle Flow

Shows what happens during `feast apply` and `get_online_features`, and how version
history, pinning, and version-qualified reads fit together.

```
                             feast apply
                                 |
                                 v
                    +------------------------+
                    |  Compare new definition |
                    |  against active FV      |
                    +------------------------+
                           |            |
                    schema/UDF     metadata only
                     changed        changed
                           |            |
                           v            v
                  +--------------+  +------------------+
                  | Save old as  |  | Update in place, |
                  | version N    |  | no new version   |
                  | Save new as  |  +------------------+
                  | version N+1  |
                  +--------------+
                           |
              +------------+------------+
              |                         |
              v                         v
     +----------------+      +-------------------+
     |    Registry     |      |   Online Store    |
     | (version        |      | (only if flag on) |
     |  history)       |      +-------------------+
     +----------------+              |
              |               +------+------+
              |               |             |
              v               v             v
     +----------------+  +--------+  +-----------+
     | feast versions |  | proj_  |  | proj_     |
     | feast pin v2   |  | fv     |  | fv_v1     |
     | list / get     |  | (v0)   |  | fv_v2 ... |
     +----------------+  +--------+  +-----------+
      Always available     Unversioned   Versioned
                            table        tables


                       get_online_features
                              |
                              v
                   +---------------------+
                   | Parse feature refs  |
                   +---------------------+
                     |                 |
             "fv:feature"      "fv@v2:feature"
             (no version)      (version-qualified)
                     |                 |
                     v                 v
              +------------+   +------------------+
              | Read from  |   | flag enabled?    |
              | active FV  |   +------------------+
              | table      |     |            |
              +------------+    yes           no
                                 |            |
                                 v            v
                          +------------+  +-------+
                          | Look up v2 |  | raise |
                          | snapshot,  |  | error |
                          | read from  |  +-------+
                          | proj_fv_v2 |
                          +------------+
```

### Architecture / Storage

Shows how version data is stored in the registry and online store, and the
relationship between the active definition and historical snapshots.

```
+--feature_store.yaml------------------------------------------+
|  registry:                                                   |
|    path: data/registry.db                                    |
|    enable_online_feature_view_versioning: true   (optional)  |
+--------------------------------------------------------------+
         |                                    |
         v                                    v
+--Registry (file or SQL)--+     +--Online Store (SQLite, ...)---+
|                          |     |                               |
|  Active Feature Views    |     |  Unversioned tables (v0)      |
|  +--------------------+  |     |  +-------------------------+  |
|  | driver_stats       |  |     |  | proj_driver_stats       |  |
|  |   version: latest  |  |     |  |   driver_id | trips | . |  |
|  |   current_ver: 2   |  |     |  +-------------------------+  |
|  |   schema: [...]    |  |     |                               |
|  +--------------------+  |     |  Versioned tables (v1+)       |
|                          |     |  +-------------------------+  |
|  Version History         |     |  | proj_driver_stats_v1    |  |
|  +--------------------+  |     |  |   driver_id | trips | . |  |
|  | v0: proto snapshot |  |     |  +-------------------------+  |
|  |     created: Jan 15 |  |     |  +-------------------------+  |
|  | v1: proto snapshot |  |     |  | proj_driver_stats_v2    |  |
|  |     created: Jan 16 |  |     |  |   driver_id | trips | . |  |
|  | v2: proto snapshot |  |     |  +-------------------------+  |
|  |     created: Jan 20 |  |     |                               |
|  +--------------------+  |     +-------------------------------+
|                          |
|  Always active.          |     Only created when flag is on
|  No flag needed.         |     and feast materialize is run.
+--------------------------+
```

## Design

### Core Concepts

- **Version number**: An auto-incrementing integer (v0, v1, v2, ...) assigned to each schema-significant change.
- **Version snapshot**: A serialized copy of the full feature view proto at that version, stored in the registry's version history table.
- **Version pin**: Setting `version="v2"` on a feature view replaces the active definition with the v2 snapshot — essentially a revert.
- **Version-qualified ref**: The `@v<N>` syntax in feature references (e.g., `driver_stats@v2:trips_today`) for reading from a specific version's online store table.

### What Triggers a New Version

Only **schema and UDF changes** create new versions. Metadata-only changes (description, tags, owner, TTL, online/offline flags) update the active definition in place without creating a version.

Schema-significant changes include:
- Adding, removing, or retyping feature columns
- Changing entities or entity columns
- Changing the UDF code (StreamFeatureView, OnDemandFeatureView)

This keeps version history meaningful — a new version number always means a real structural change.

### What Does NOT Trigger a New Version

- Re-applying an identical definition (idempotent)
- Changing `description`, `tags`, `owner`
- Changing `ttl`, `online`, `offline` flags
- Changing data source paths/locations (treated as deployment config)

### Version History Is Always-On

Version history tracking is lightweight registry metadata — just a serialized proto snapshot per version. There is no performance cost to the online path and no additional infrastructure required. For this reason, version history is **always active** with no opt-in flag needed.

Out of the box, every `feast apply` that changes a feature view will:
- Record a version snapshot
- Support `feast feature-views list-versions <name>` to list history
- Support `registry.list_feature_view_versions(name, project)` programmatically
- Support `registry.get_feature_view_by_version(name, project, version_number)` for snapshot retrieval
- Support version pinning via `version="v2"` in feature view definitions

### Online Versioning Is Opt-In

The expensive/risky part of versioning is creating **separate online store tables per version** and routing reads to them. This is gated behind a config flag:

```yaml
registry:
  path: data/registry.db
  enable_online_feature_view_versioning: true
```

When enabled, version-qualified refs like `driver_stats@v2:trips_today` in `get_online_features()` will:
1. Look up the v2 snapshot from version history
2. Read from a version-specific online store table (`project_driver_stats_v2`)

When disabled (the default), using `@v<N>` refs raises a clear error. All other versioning features (history, listing, pinning, snapshot retrieval) work regardless.

### Storage

**File-based registry**: Version history is stored as a repeated `FeatureViewVersionRecord` message in the registry proto, alongside the existing feature view definitions.

**SQL registry**: A dedicated `feature_view_version_history` table with columns for name, project, version number, type, proto bytes, and creation timestamp.

### Version Pinning

Pinning replaces the active feature view with a historical snapshot:

```python
driver_stats = FeatureView(
    name="driver_stats",
    entities=[driver],
    schema=[...],
    source=my_source,
    version="v2",  # revert to v2's definition
)
```

Safety constraints:
- The user's feature view definition (minus the version field) must match the currently active definition. If the user changed both the schema and the version pin simultaneously, `feast apply` raises `FeatureViewPinConflict`. This prevents accidental "I thought I was reverting but I also changed things."
- Pinning does not modify version history — v0, v1, v2 snapshots remain intact.
- After a pin, removing the version field (or setting `version="latest"`) returns to auto-incrementing behavior. If the next `feast apply` detects a schema change, a new version is created.

### Version-Qualified Feature References

The `@v<N>` syntax extends the existing `feature_view:feature` reference format:

```python
features = store.get_online_features(
    features=[
        "driver_stats:trips_today",         # latest (default)
        "driver_stats@v2:trips_today",      # read from v2
        "driver_stats@v1:avg_rating",       # read from v1
    ],
    entity_rows=[{"driver_id": 1001}],
)
```

Online store table naming:
- v0 uses the existing unversioned table (`project_driver_stats`) for backward compatibility
- v1+ use suffixed tables (`project_driver_stats_v1`, `project_driver_stats_v2`)

Each version requires its own materialization. `@latest` always resolves to the active version.

### Supported Feature View Types

Versioning works on all three feature view types:
- `FeatureView` / `BatchFeatureView`
- `StreamFeatureView`
- `OnDemandFeatureView`

### Online Store Support

Version-qualified reads (`@v<N>`) are currently implemented for the **SQLite** online store. Other online stores will raise a clear error. Expanding to additional stores is follow-up work.

### Materialization

Each version's data lives in its own online store table (e.g., `project_fv_v1`, `project_fv_v2`). By default, `feast materialize` and `feast materialize-incremental` populate the **active (latest)** version's table. To populate a specific version's table, pass the `--version` flag along with a single `--views` target:

```bash
# Materialize v1 of driver_stats
feast materialize --views driver_stats --version v1 2024-01-01T00:00:00 2024-01-15T00:00:00

# Incrementally materialize v2 of driver_stats
feast materialize-incremental --views driver_stats --version v2 2024-01-15T00:00:00
```

Python SDK equivalent:

```python
store.materialize(
    feature_views=["driver_stats"],
    version="v2",
    start_date=start,
    end_date=end,
)
```

**Requirements:**
- `enable_online_feature_view_versioning: true` must be set in `feature_store.yaml`
- `--version` requires `--views` with exactly one feature view name
- The specified version must exist in the registry (created by a prior `feast apply`)
- Without `--version`, materialization targets the active version's table (existing behavior)

**Multi-version workflow example:**

```bash
# Model A uses v1, Model B uses v2 — populate both tables
feast materialize --views driver_stats --version v1 2024-01-01T00:00:00 2024-02-01T00:00:00
feast materialize --views driver_stats --version v2 2024-01-01T00:00:00 2024-02-01T00:00:00

# Models can now query their respective versions online
# Model A: store.get_online_features(features=["driver_stats@v1:trips_today"], ...)
# Model B: store.get_online_features(features=["driver_stats@v2:trips_today"], ...)
```

## API Surface

### Python SDK

```python
# List version history
versions = store.list_feature_view_versions("driver_stats")
# [{"version": "v0", "version_number": 0, "created_timestamp": ..., ...}, ...]

# Get a specific version's definition
fv_v1 = store.registry.get_feature_view_by_version("driver_stats", project, 1)

# Pin to a version
FeatureView(name="driver_stats", ..., version="v2")

# Version-qualified online read (requires enable_online_feature_view_versioning)
store.get_online_features(features=["driver_stats@v2:trips_today"], ...)

# Materialize a specific version
store.materialize(feature_views=["driver_stats"], version="v2", start_date=start, end_date=end)
store.materialize_incremental(feature_views=["driver_stats"], version="v2", end_date=end)
```

### CLI

```bash
# List versions
feast feature-views list-versions driver_stats

# Output:
# VERSION  TYPE          CREATED              VERSION_ID
# v0       feature_view  2024-01-15 10:30:00  a1b2c3d4-...
# v1       feature_view  2024-01-16 14:22:00  e5f6g7h8-...

# Materialize a specific version
feast materialize --views driver_stats --version v2 2024-01-01T00:00:00 2024-02-01T00:00:00
feast materialize-incremental --views driver_stats --version v2 2024-02-01T00:00:00
```

### Configuration

```yaml
# feature_store.yaml
registry:
  path: data/registry.db
  # Optional: enable versioned online tables and @v<N> reads (default: false)
  enable_online_feature_view_versioning: true
```

## Migration & Backward Compatibility

- **Zero breaking changes.** All existing feature views continue to work. The `version` parameter defaults to `"latest"` and `current_version_number` defaults to `None`.
- **Existing online data is preserved.** The unversioned online store table is treated as v0. No data migration needed.
- **Version history starts on first apply.** Pre-existing feature views get a v0 snapshot on their next `feast apply`.
- **Proto backward compatibility.** The new `version` and `current_version_number` fields use proto defaults (empty string and 0) so old protos deserialize correctly.

## Concurrency

Two concurrent `feast apply` calls on the same feature view can race on version number assignment. The behavior depends on the version mode and registry backend.

### `version="latest"` (auto-increment)

The registry computes `MAX(version_number) + 1` and saves the new snapshot. If two concurrent applies race on the same version number:

- **SQL registry**: The unique constraint on `(feature_view_name, project_id, version_number)` causes an `IntegrityError`. The registry catches this and retries up to 3 times, re-reading `MAX + 1` each time. Since the client said "latest", the exact version number doesn't matter.
- **File registry**: Last-write-wins. The file registry uses an in-memory proto with no database-level constraints, so concurrent writes may overwrite each other. This is a pre-existing limitation for all file registry operations.

### `version="v<N>"` (explicit version)

The registry checks whether version N already exists:

- **Exists** → pin/revert to that version's snapshot (unchanged behavior)
- **Doesn't exist** → forward declaration: create version N with the provided definition

If two concurrent applies both try to forward-declare the same version:

- **SQL registry**: The first one succeeds; the second gets a `ConcurrentVersionConflict` error with a clear message to pull latest and retry.
- **File registry**: Last-write-wins (same pre-existing limitation).

### Recommendations

- For single-developer or CI/CD workflows, the file registry works fine.
- For multi-client environments with concurrent applies, use the SQL registry for proper conflict detection.

## Feature Services

Feature services work with versioned feature views when the online versioning flag is enabled:

- **Automatic version resolution.** When `enable_online_feature_view_versioning` is `true` and a feature service references a versioned feature view (`current_version_number > 0`), the serving path automatically sets `version_tag` on the projection. This ensures `get_online_features()` reads from the correct versioned online store table (e.g., `project_driver_stats_v1`) instead of the unversioned table.
- **Version-qualified feature refs.** Both `_get_features()` and `_get_feature_views_to_use()` produce version-qualified keys (e.g., `driver_stats@v1:trips_today`) for feature services referencing versioned FVs, keeping the feature ref index and the FV lookup index in sync.
- **Gated by flag.** If any feature view referenced by a feature service has been versioned (`current_version_number > 0`) but `enable_online_feature_view_versioning` is `false`:
  - `feast apply` will reject the feature service with a clear error.
  - `get_online_features()` will fail at retrieval time with a descriptive error message.
- **No `@v<N>` syntax in feature services.** Version-qualified reads (`driver_stats@v2:trips_today`) using the `@v<N>` syntax require string-based feature references passed directly to `get_online_features()`. Feature services always resolve to the active (latest) version of each referenced feature view.
- **Future work: per-reference version pinning.** A future enhancement could allow feature services to pin individual feature view references to specific versions (e.g., `FeatureService(features=[driver_stats["v2"]])`).

## Limitations & Future Work

- **Online store coverage.** Version-qualified reads are only on SQLite today. Redis, DynamoDB, Bigtable, Postgres, etc. are follow-up work.
- **Offline store versioning.** This RFC covers online reads only. Versioned historical retrieval is out of scope.
- **Version deletion.** There is no mechanism to prune old versions. This could be added later if registries grow large.
- **Cross-version joins.** Joining features from different versions of the same feature view in `get_historical_features` is not supported.

## Open Questions

1. **Should version history have a retention policy?** For long-lived feature views with frequent schema changes, version history could grow unbounded. A `max_versions` config or TTL-based pruning could help.
2. **Should version-qualified refs work in `get_historical_features`?** The current implementation is online-only. Offline versioned reads would require point-in-time-correct version resolution.
3. **Should we support version aliases?** e.g., `driver_stats@stable:trips` mapping to a pinned version number via config.

## References

- Branch: `featureview-versioning`
- Documentation: `docs/getting-started/concepts/feature-view.md` (Versioning section)
- Tests: `sdk/python/tests/integration/registration/test_versioning.py`, `sdk/python/tests/unit/test_feature_view_versioning.py`
