# \[Alpha\] Feature View Versioning

{% hint style="warning" %}
**Warning**: This is an _experimental_ feature. It is stable but there are still rough edges. Contributions are welcome!
{% endhint %}

## Overview

Feature view versioning automatically tracks schema and UDF changes to feature views. Every time `feast apply` detects a change, a versioned snapshot is saved to the registry. This enables:

- **Audit trail** — see what a feature view looked like at any point in time
- **Safe rollback** — pin serving to a prior version with `version="v0"` in your definition
- **Multi-version serving** — serve both old and new schemas simultaneously using `@v<N>` syntax
- **Staged publishing** — use `feast apply --no-promote` to publish a new version without making it the default

## How It Works

Version tracking is fully automatic. You don't need to set any version parameter — just use `feast apply` as usual:

1. **First apply** — Your feature view definition is saved as **v0**.
2. **Change something and re-apply** — Feast detects the change, saves the old definition as a snapshot, and saves the new one as **v1**. The version number auto-increments on each real change.
3. **Re-apply without changes** — Nothing happens. Feast compares the new definition against the active one and skips creating a version if they're identical (idempotent).
4. **Another change** — Creates **v2**, and so on.

```
feast apply                  # First apply → v0
# ... edit schema ...
feast apply                  # Detects change → v1
feast apply                  # No change detected → still v1 (no new version)
# ... edit source ...
feast apply                  # Detects change → v2
```

**Key details:**

* **Automatic snapshots**: Versions are created only when Feast detects an actual change to the feature view definition (schema or UDF). Metadata-only changes (description, tags, TTL) update in place without creating a new version.
* **Separate history storage**: Version history is stored separately from the active feature view definition, keeping the main registry lightweight.
* **Backward compatible**: The `version` parameter is fully optional. Omitting it (or setting `version="latest"`) preserves existing behavior — you get automatic versioning with zero changes to your code.

## Configuration

{% hint style="info" %}
Version history tracking is **always active** — no configuration needed. Every `feast apply` that changes a feature view automatically records a version snapshot.

To enable **versioned online reads** (e.g., `fv@v2:feature`), add `enable_online_feature_view_versioning: true` to your registry config in `feature_store.yaml`:

```yaml
registry:
  path: data/registry.db
  enable_online_feature_view_versioning: true
```

When this flag is off, version-qualified refs (e.g., `fv@v2:feature`) in online reads will raise errors, but version history, version listing, version pinning, and version lookups all work normally.
{% endhint %}

## Pinning to a Specific Version

You can pin a feature view to a specific historical version by setting the `version` parameter. When pinned, `feast apply` replaces the active feature view with the snapshot from that version. This is useful for reverting to a known-good definition.

```python
from feast import FeatureView

# Default behavior: always use the latest version (auto-increments on changes)
driver_stats = FeatureView(
    name="driver_stats",
    entities=[driver],
    schema=[...],
    source=my_source,
)

# Pin to a specific version (reverts the active definition to v2's snapshot)
driver_stats = FeatureView(
    name="driver_stats",
    entities=[driver],
    schema=[...],
    source=my_source,
    version="v2",  # also accepts "version2"
)
```

When pinning, the feature view definition (schema, source, transformations, etc.) must match the currently active definition. If you've also modified the definition alongside the pin, `feast apply` will raise a `FeatureViewPinConflict` error. To apply changes, use `version="latest"`. To revert, only change the `version` parameter.

The snapshot's content replaces the active feature view. Version history is not modified by a pin; the existing v0, v1, v2, etc. snapshots remain intact.

After reverting with a pin, you can go back to normal auto-incrementing behavior by removing the `version` parameter (or setting it to `"latest"`) and running `feast apply` again. If the restored definition differs from the pinned snapshot, a new version will be created.

### Version string formats

| Format | Meaning |
|--------|---------|
| `"latest"` (or omitted) | Always use the latest version (auto-increments on changes) |
| `"v0"`, `"v1"`, `"v2"`, ... | Pin to a specific version number |
| `"version0"`, `"version1"`, ... | Equivalent long form (case-insensitive) |

## Staged Publishing (`--no-promote`)

By default, `feast apply` atomically saves a version snapshot **and** promotes it to the active definition. For breaking schema changes, you may want to stage the new version without disrupting unversioned consumers.

The `--no-promote` flag saves the version snapshot without updating the active feature view definition. The new version is accessible only via explicit `@v<N>` reads and `--version` materialization.

**CLI usage:**

```bash
feast apply --no-promote
```

**Python SDK equivalent:**

```python
store.apply([entity, feature_view], no_promote=True)
```

### Phased rollout workflow

1. **Stage the new version:**
   ```bash
   feast apply --no-promote
   ```
   This publishes v2 without promoting it. All unversioned consumers continue using v1.

2. **Populate the v2 online table:**
   ```bash
   feast materialize --views driver_stats --version v2 ...
   ```

3. **Migrate consumers one at a time:**
   - Consumer A switches to `driver_stats@v2:trips_today`
   - Consumer B switches to `driver_stats@v2:avg_rating`

4. **Promote v2 as the default:**
   ```bash
   feast apply
   ```
   Or pin to v2: set `version="v2"` in the definition and run `feast apply`.

## Listing Version History

Use the CLI to inspect version history:

```bash
feast feature-views list-versions driver_stats
```

```text
VERSION  TYPE          CREATED              VERSION_ID
v0       feature_view  2024-01-15 10:30:00  a1b2c3d4-...
v1       feature_view  2024-01-16 14:22:00  e5f6g7h8-...
v2       feature_view  2024-01-20 09:15:00  i9j0k1l2-...
```

Or programmatically via the Python SDK:

```python
store = FeatureStore(repo_path=".")
versions = store.list_feature_view_versions("driver_stats")
for v in versions:
    print(f"{v['version']} created at {v['created_timestamp']}")
```

## Version-Qualified Feature References

You can read features from a **specific version** of a feature view by using version-qualified feature references with the `@v<N>` syntax:

```python
online_features = store.get_online_features(
    features=[
        "driver_stats:trips_today",           # latest version (default)
        "driver_stats@v2:trips_today",        # specific version
        "driver_stats@latest:trips_today",    # explicit latest
    ],
    entity_rows=[{"driver_id": 1001}],
)
```

**How it works:**

* `driver_stats:trips_today` is equivalent to `driver_stats@latest:trips_today` — it reads from the currently active version
* `driver_stats@v2:trips_today` reads from the v2 snapshot stored in version history, using a version-specific online store table
* Multiple versions of the same feature view can be queried in a single request (e.g., `driver_stats@v1:trips` and `driver_stats@v2:trips_daily`)

**Backward compatibility:**

* The unversioned online store table (e.g., `project_driver_stats`) is treated as v0
* Only versions >= 1 get `_v{N}` suffixed tables (e.g., `project_driver_stats_v1`)
* Pre-versioning users' existing data continues to work without changes — `@latest` resolves to the active version, which for existing unversioned FVs is v0

**Materialization:** Each version requires its own materialization. After applying a new version, run `feast materialize` to populate the versioned table before querying it with `@v<N>`.

## Supported Feature View Types

Versioning is supported on all three feature view types:

* `FeatureView` (and `BatchFeatureView`)
* `StreamFeatureView`
* `OnDemandFeatureView`

## Online Store Support

{% hint style="info" %}
**Currently, version-qualified online reads (`@v<N>`) are only supported with the SQLite online store.** Support for additional online stores (Redis, DynamoDB, Bigtable, Postgres, etc.) will be added based on community priority.

If you need versioned online reads for a specific online store, please [open a GitHub issue](https://github.com/feast-dev/feast/issues/new) describing your use case and which store you need. This helps us prioritize development.
{% endhint %}

Version history tracking in the registry (listing versions, pinning, `--no-promote`) works with **all** registry backends (file, SQL, Snowflake).

## Full Details

For the complete design, concurrency semantics, and feature service interactions, see the [Feature View Versioning RFC](../adr/feature-view-versioning.md).

## Naming Restrictions

Feature references use a structured format: `feature_view_name@v<N>:feature_name`. To avoid
ambiguity, the following characters are reserved and must not appear in feature view or feature names:

- **`@`** — Reserved as the version delimiter (e.g., `driver_stats@v2:trips_today`). `feast apply`
  will reject feature views with `@` in their name. If you have existing feature views with `@` in
  their names, they will continue to work for unversioned reads, but we recommend renaming them to
  avoid ambiguity with the `@v<N>` syntax.
- **`:`** — Reserved as the separator between feature view name and feature name in fully qualified
  feature references (e.g., `driver_stats:trips_today`).

## Known Limitations

- **Online store coverage** — Version-qualified reads (`@v<N>`) are SQLite-only today. Other online stores are follow-up work.
- **Offline store versioning** — Versioned historical retrieval is not yet supported.
- **Version deletion** — There is no mechanism to prune old versions from the registry.
- **Cross-version joins** — Joining features from different versions of the same feature view in `get_historical_features` is not supported.
- **Feature services** — Feature services always resolve to the active (promoted) version. `--no-promote` versions are not served until promoted.
