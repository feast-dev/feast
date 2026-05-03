---
name: feast-testing
description: How to test and debug Feast — running targeted tests, writing unit tests for new components, debugging registry and online store issues, and inspecting live feature store state. Use when writing tests for a new feature, debugging a failing test, investigating a runtime error, or verifying that a change works correctly end-to-end.
license: Apache-2.0
compatibility: Works with Claude Code, OpenAI Codex, and any Agent Skills compatible tool.
metadata:
  author: feast-dev
  version: "1.0"
---

# Testing and Debugging Feast

## Test Organization

```
sdk/python/tests/
├── unit/                        # Fast tests, no external deps, run locally
│   ├── infra/
│   │   ├── online_store/        # Per-store unit tests (redis, dynamodb, etc.)
│   │   ├── offline_stores/      # Offline store unit tests
│   │   └── registry/            # Registry unit tests
│   ├── test_unit_feature_store.py  # Core FeatureStore behavior
│   ├── test_feature_views.py    # Feature view validation
│   └── test_on_demand_*.py      # On-demand transformation tests
└── integration/                 # Requires real infrastructure (run in CI)
    ├── feature_repos/           # Sample repos used as test fixtures
    └── test_universal_*.py      # Cross-store compatibility tests
```

**Rule of thumb**: if a test needs mocks for an external service, it belongs in `unit/`.
If it spins up real infra (Redis, DynamoDB, BigQuery), it belongs in `integration/`.

---

## Running Tests

### Targeted unit tests

```bash
# Run a single test file
python -m pytest sdk/python/tests/unit/infra/online_store/test_dynamodb_online_store.py -v

# Run a single test by name
python -m pytest sdk/python/tests/unit/test_unit_feature_store.py -k "test_apply" -v

# Run all unit tests for one subsystem
python -m pytest sdk/python/tests/unit/infra/online_store/ -v

# Run all unit tests (fast, no infra needed)
make test-python-unit
```

### Integration tests (local)

```bash
# Run local integration tests (SQLite online, file offline)
make test-python-integration-local

# Run specific integration test with verbose output
python -m pytest sdk/python/tests/integration/ -k "test_online_retrieval" -v -s
```

### Running a single test file fast (skip slow markers)

```bash
python -m pytest sdk/python/tests/unit/infra/online_store/test_redis.py -v --no-header -q
```

---

## Writing Unit Tests for a New Component

### Pattern: testing an online store

Follow `sdk/python/tests/unit/infra/online_store/test_dynamodb_online_store.py` or `test_redis.py`.

```python
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from feast.infra.online_stores.mystore import MyStore, MyStoreConfig
from feast.repo_config import RepoConfig

def make_config(store_config: dict) -> RepoConfig:
    return RepoConfig(
        project="test_project",
        registry="data/registry.db",
        provider="local",
        online_store=store_config,
    )

@patch("feast.infra.online_stores.mystore.MyClient")
def test_online_write_batch(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    config = make_config({"type": "mystore", "host": "localhost"})
    store = MyStore()
    feature_view = MagicMock()
    feature_view.name = "driver_stats"

    store.online_write_batch(config, feature_view, data=[...], progress=None)

    mock_client.set.assert_called_once()

# For async methods:
def test_online_write_batch_async():
    async def _run():
        store = MyStore()
        await store.online_write_batch_async(config, feature_view, data, progress=None)
    asyncio.run(_run())
```

### Pattern: testing registry operations

Follow `sdk/python/tests/unit/infra/registry/test_registry.py`.

```python
from feast.infra.registry.registry import Registry
from feast.repo_config import RegistryConfig

def test_apply_feature_view(tmp_path):
    config = RegistryConfig(path=str(tmp_path / "registry.db"))
    registry = Registry("test_project", config, repo_path=None)

    entity = Entity(name="driver_id", value_type=ValueType.INT64)
    registry.apply_entity(entity, project="test_project")

    stored = registry.get_entity("driver_id", project="test_project")
    assert stored.name == "driver_id"
```

### Pattern: testing FeatureStore end-to-end (unit level)

```python
from feast import FeatureStore
from feast.repo_config import RepoConfig

def test_get_online_features(tmp_path):
    config = RepoConfig(
        project="test",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
        offline_store={"type": "file"},
    )
    store = FeatureStore(config=config)
    # apply objects, write data, then assert retrieval
```

---

## Debugging Common Issues

### Registry out of sync / stale metadata

```bash
# Inspect the live registry
feast registry-dump  # prints full proto as JSON

# Or from Python:
from feast import FeatureStore
store = FeatureStore(repo_path=".")
store.registry.list_feature_views(project=store.project)
store.registry.list_entities(project=store.project)
```

**Symptom**: `FeatureView not found` or stale feature definitions.
**Fix**: Re-run `feast apply`. If using a cached registry (TTL > 0), wait for cache expiry or force refresh by restarting the process.

### Online store returns None for all features

```bash
# Check if materialization has run
feast feature-views list     # shows last_updated_timestamp

# Check entity key format
# Entity keys are serialized — if the serialization_version doesn't match, lookups fail.
# Check entity_key_serialization_version in feature_store.yaml
```

**Symptom**: `get_online_features` returns all `None` values despite materialization running.
**Cause**: Entity key serialization version mismatch, wrong project name, or wrong TTL causing data to expire.

### Type errors from mypy

```bash
# Type-check only the file you changed
uv run bash -c "cd sdk/python && mypy feast/infra/online_stores/mystore.py"

# Common fix: add Optional[] around return types, use cast() for dynamic lookups
```

### Test imports fail / module not found

```bash
# Ensure you're running from repo root with dev dependencies installed
make install-python-dependencies-dev

# Run with uv to use the pinned venv
uv run python -m pytest sdk/python/tests/unit/... -v
```

### Protobuf deserialization errors

**Symptom**: `DecodeError` or unexpected field values when reading registry.
**Cause**: Proto schema changed without recompiling.
**Fix**:
```bash
make compile-protos-python
```

### Feature server returns 500

```bash
# Start the feature server locally and check logs
feast serve --host 0.0.0.0 --port 6566

# Test directly
curl -X POST http://localhost:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{"features": ["driver_stats:conv_rate"], "entities": {"driver_id": [1001]}}'
```

---

## Useful CLI Commands for Inspection

```bash
feast entities list                  # list all registered entities
feast feature-views list             # list feature views + last materialization time
feast feature-services list          # list feature services
feast on-demand-feature-views list   # list ODFVs
feast registry-dump                  # dump full registry as JSON (debug)
feast plan                           # preview what feast apply would change (dry run)
```

---

## Test Utilities

| Utility | Location | Purpose |
|---|---|---|
| `get_feature_view()` helpers | `tests/unit/infra/online_store/test_*.py` | Build minimal FeatureView mocks |
| `LocalRegistry` fixture | `tests/unit/infra/registry/test_registry.py` | In-memory registry for unit tests |
| `integration/feature_repos/` | `tests/integration/` | Sample repos for integration test scenarios |
| `conftest.py` fixtures | Various `tests/` subdirs | Shared pytest fixtures per subsystem |
