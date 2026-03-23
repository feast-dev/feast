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

## Quick Start

Version history tracking is **always active** with no configuration required. Every `feast apply` that changes a feature view automatically records a version snapshot.

### List versions

```bash
feast version-history driver_stats
```

### Pin to a prior version

```python
driver_stats = FeatureView(
    name="driver_stats",
    version="v0",  # Pin to v0
    ...
)
```

### Staged publishing (no-promote)

```bash
# Publish v2 without promoting it to active
feast apply --no-promote

# Populate v2 online table
feast materialize --views driver_stats --version v2 ...

# Migrate consumers to @v2 refs, then promote
feast apply
```

### Version-qualified online reads

To enable version-qualified reads (e.g., `driver_stats@v2:trips_today`), add the following to your `feature_store.yaml`:

```yaml
registry:
  path: data/registry.db
  enable_online_feature_view_versioning: true
```

Then query specific versions:

```python
features = store.get_online_features(
    features=["driver_stats@v2:trips_today"],
    entity_rows=[{"driver_id": 1001}],
)
```

## Online Store Support

{% hint style="info" %}
**Currently, version-qualified online reads (`@v<N>`) are only supported with the SQLite online store.** Support for additional online stores (Redis, DynamoDB, Bigtable, Postgres, etc.) will be added based on community priority.

If you need versioned online reads for a specific online store, please [open a GitHub issue](https://github.com/feast-dev/feast/issues/new) describing your use case and which store you need. This helps us prioritize development.
{% endhint %}

Version history tracking in the registry (listing versions, pinning, `--no-promote`) works with **all** registry backends (file, SQL, Snowflake).

## Full Details

For the complete design, concurrency semantics, and feature service interactions, see the [Feature View Versioning RFC](../rfcs/feature-view-versioning.md).

## Known Limitations

- **Online store coverage** — Version-qualified reads (`@v<N>`) are SQLite-only today. Other online stores are follow-up work.
- **Offline store versioning** — Versioned historical retrieval is not yet supported.
- **Version deletion** — There is no mechanism to prune old versions from the registry.
- **Cross-version joins** — Joining features from different versions of the same feature view in `get_historical_features` is not supported.
- **Feature services** — Feature services always resolve to the active (promoted) version. `--no-promote` versions are not served until promoted.
