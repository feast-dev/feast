# ADR-0008: Feature View Versioning

## Status

Accepted

## Context

When a feature view's schema changed in Feast, the old definition was silently overwritten. This created several problems:

1. **No audit trail**: Teams couldn't answer "what did this feature view look like last week?" or "who changed the schema and when?"
2. **No safe rollback**: If a schema change broke a downstream model, there was no way to revert without manually reconstructing the previous definition.
3. **No multi-version serving**: During migrations, teams often need to serve both old and new schemas simultaneously (e.g., model A uses v1 features, model B uses v2 features). This required creating entirely separate feature views.

## Decision

Add automatic version tracking to Feast feature views. Every time `feast apply` detects a schema or UDF change, a versioned snapshot is saved to the registry.

### Core Concepts

- **Version number**: Auto-incrementing integer (v0, v1, v2, ...) for each schema-significant change.
- **Version snapshot**: Serialized copy of the full feature view proto stored in the registry.
- **Version pin**: Setting `version="v2"` on a feature view replaces the active definition with the v2 snapshot.
- **Version-qualified ref**: The `@v<N>` syntax (e.g., `driver_stats@v2:trips_today`) for reading from a specific version.

### What Triggers a New Version

Only schema and UDF changes create new versions:

- Adding, removing, or retyping feature columns.
- Changing entities or entity columns.
- Changing UDF code (StreamFeatureView, OnDemandFeatureView).

Metadata-only changes (description, tags, owner, TTL) update the active definition in place without creating a version.

### Version History Is Always-On

Version history tracking is lightweight registry metadata (serialized proto snapshots). There is no performance cost to the online path. Every `feast apply` that changes a feature view will:

- Record a version snapshot.
- Support `feast feature-views list-versions <name>` to list history.
- Support `registry.list_feature_view_versions(name, project)` programmatically.
- Support `registry.get_feature_view_by_version(name, project, version_number)`.

### Online Versioning Is Opt-In

Version-qualified reads from separate online store tables are gated behind a config flag:

```yaml
registry:
  path: data/registry.db
  enable_online_feature_view_versioning: true
```

When enabled, `driver_stats@v2:trips_today` reads from a version-specific table (`project_driver_stats_v2`). When disabled (default), using `@v<N>` refs raises a clear error.

### Version Pinning

```python
driver_stats = FeatureView(
    name="driver_stats",
    entities=[driver],
    schema=[...],
    source=my_source,
    version="v2",  # revert to v2's definition
)
```

Safety: The user's definition (minus the version field) must match the currently active definition. If both schema and version pin are changed, `feast apply` raises `FeatureViewPinConflict`.

### Staged Publishing (`--no-promote`)

The `--no-promote` flag saves a version snapshot without updating the active definition, enabling phased rollouts:

```bash
# Stage the new version
feast apply --no-promote

# Populate the v2 online table
feast materialize --views driver_stats --version v2 ...

# Migrate consumers one at a time (using @v2 refs)

# Promote v2 as the default
feast apply
```

### Materialization

Each version's data lives in its own online store table. By default, `feast materialize` targets the active version. A `--version` flag targets specific versions:

```bash
feast materialize --views driver_stats --version v1 2024-01-01T00:00:00 2024-01-15T00:00:00
```

### Concurrency

- **SQL registry**: Unique constraint on `(feature_view_name, project_id, version_number)` with retry logic for auto-increment races.
- **File registry**: Last-write-wins (pre-existing limitation).

### Limitations

- Online store coverage: Version-qualified reads are only on SQLite initially.
- Offline store versioning is out of scope.
- No mechanism to prune old versions.
- Cross-version joins in `get_historical_features` are not supported.

## Consequences

### Positive

- Full audit trail of schema changes for every feature view.
- Safe rollback capability through version pinning.
- Multi-version serving enables gradual migrations without creating duplicate feature views.
- Always-on history tracking with zero performance cost to the online path.
- Staged publishing supports safe, phased rollouts of breaking changes.

### Negative

- Version-qualified online reads are initially limited to SQLite.
- Offline versioning is not supported, creating a gap for reproducing historical training data.
- No version pruning mechanism may lead to unbounded growth in long-lived feature views.
- Concurrency handling differs between SQL and file registries.

## References

- Original RFC: [Feature View Versioning](https://docs.google.com/document/d/1OE-S-10kdBwxWHG4TI_zdg_VAQNST38IkSVmQkCfjeQ/edit)
- Pull Request: [#6101](https://github.com/feast-dev/feast/pull/6101)
- Implementation: `sdk/python/feast/feature_view.py` (version fields), `docs/adr/feature-view-versioning.md`
