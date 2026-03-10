---
name: feast-dev
description: Development guide for contributing to the Feast codebase. Covers environment setup, testing, linting, project structure, and PR workflow for feast-dev/feast.
license: Apache-2.0
compatibility: Works with Claude Code, OpenAI Codex, and any Agent Skills compatible tool. Requires Python 3.10+, uv, and git.
metadata:
  author: feast-dev
  version: "1.0"
---

# Feast Development Guide

## Environment Setup

```bash
# Install uv (if not installed)
pip install uv

# Create virtual environment and install Feast in editable mode with dev dependencies
uv pip install -e ".[dev]"

# Install pre-commit hooks (runs formatters and linters on commit)
make install-precommit
```

## Running Tests

### Unit Tests
```bash
# Run all unit tests
make test-python-unit

# Run a specific test file
python -m pytest sdk/python/tests/unit/test_feature_store.py -v

# Run a specific test
python -m pytest sdk/python/tests/unit/test_feature_store.py::TestFeatureStore::test_apply -v
```

### Integration Tests (local)
```bash
# Start local test infrastructure
make start-local-integration-tests

# Run integration tests
make test-python-integration-local
```

## Linting and Formatting

```bash
# Run all linters
make lint

# Auto-format code
make format

# Type checking
mypy sdk/python/feast
```

## Code Style

- Use type hints on all function signatures
- Use `from __future__ import annotations` at the top of new files
- Follow existing patterns in the module you are modifying
- PR titles must follow semantic conventions: `feat:`, `fix:`, `ci:`, `chore:`, `docs:`
- Add a GitHub label to PRs (e.g. `kind/bug`, `kind/feature`, `kind/housekeeping`)
- Sign off commits with `git commit -s` (DCO requirement)

## Project Structure

```
sdk/python/feast/          # Main Python SDK
  cli.py                   # CLI entry point (feast apply, feast materialize, etc.)
  feature_store.py         # FeatureStore class - core orchestration
  repo_config.py           # feature_store.yaml configuration parsing
  repo_operations.py       # feast apply / feast teardown logic
  infra/                   # Online/offline store implementations
    online_stores/         # Redis, DynamoDB, SQLite, etc.
    offline_stores/        # BigQuery, Snowflake, File, etc.
  transformation/          # On-demand and streaming transformations
protos/feast/              # Protobuf definitions
sdk/python/tests/          # Test suite
  unit/                    # Fast, no external deps
  integration/             # Requires infrastructure
```

## Key Abstractions

- **FeatureStore** (`feature_store.py`): Entry point for all operations
- **FeatureView**: Defines a set of features from a data source
- **OnDemandFeatureView**: Computed features using request-time transformations
- **Entity**: Join key definition (e.g. driver_id, customer_id)
- **DataSource**: Where raw data lives (BigQuery, files, Snowflake, etc.)
- **OnlineStore**: Low-latency feature serving (Redis, DynamoDB, SQLite)
- **OfflineStore**: Historical feature retrieval (BigQuery, Snowflake, file)
