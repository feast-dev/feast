# ADR-0009: Contribution and Extensibility Architecture

## Status

Accepted

## Context

A design goal for Feast is that it should be extensible and easy to use with different technologies (storage, compute, deployment environments). After the launch of Feast 0.10, community interest grew in adding support for new online stores (Dynamo, Redis, Cassandra, HBase) and custom compute engines (Dataflow, Flink).

However, several problems existed:

1. **No decoupled interfaces**: Online stores were not decoupled from providers, so new online store contributions required building entire new providers.
2. **No contrib path**: Contributors had no way to extend the core codebase with experimental code while benefiting from the test suite.
3. **No plugin system**: No clearly defined plugin points for Providers, Offline Stores, Online Stores, and Compute, where code could live outside the Feast codebase.

## Decision

Introduce a three-tier extensibility architecture: **Interfaces**, **Contrib**, and **Plugins**.

### Interfaces

Create abstract base classes for `OnlineStore`, `OfflineStore`, and `Provider` so that different providers can reuse functionality without reimplementing it:

```
Provider (top-level orchestrator)
├── OnlineStore (abstract)
├── OfflineStore (abstract)
└── Compute (future)
```

### Contrib Module

Add a `contrib` module to the Feast SDK for community-contributed implementations:

```
feast/
└── contrib/
    ├── compute/
    │   └── spark.py
    ├── offline_stores/
    │   └── postgres.py
    ├── online_stores/
    │   ├── cassandra.py
    │   └── hbase.py
    └── providers/
        └── azure.py
```

Contrib implementations are referenced by classpath in `feature_store.yaml`:

```yaml
online_store:
  type: feast.contrib.online_stores.hbase.HbaseOnlineStore
```

Each contrib module follows a convention: a `*Config` class for configuration and a `*Test` class for setup/teardown of test infrastructure (e.g., Docker containers). Contrib code is covered by CI but failures produce warnings only.

### Plugins

External Python packages can be imported and used from within Feast without merging code upstream:

```yaml
provider:
  type: my_company_feast.MyCompanyFeastProvider
```

The key difference: contrib code is covered by Feast's test suite; external plugins are not.

## Consequences

### Positive

- Enabled a large ecosystem of community-contributed stores (Cassandra, HBase, Postgres, Spark, Trino, etc.).
- Teams can extend Feast without forking or merging code upstream.
- Clear separation between core, community-contributed, and external plugin code.
- Consistent testing patterns across all contrib implementations.

### Negative

- Contrib code may become unmaintained if original contributors disengage.
- Plugin interface requires careful versioning to avoid breaking external implementations.

## References

- Original RFC: [Feast RFC-014: Contribution Plan](https://docs.google.com/document/d/1MD0aS2_hGzd1tJ7DNjE3NgEtcuekh3O06OeQ9aavylY/edit)
- Implementation: `sdk/python/feast/infra/online_stores/`, `sdk/python/feast/infra/offline_stores/`, `sdk/python/feast/infra/contrib/`
