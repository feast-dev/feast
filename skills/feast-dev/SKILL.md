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
# Install development dependencies (uses uv pip sync with pinned requirements)
make install-python-dependencies-dev

# Install minimal dependencies
make install-python-dependencies-minimal

# Install pre-commit hooks (runs formatters and linters on commit)
make install-precommit
```

## Running Tests

### Unit Tests
```bash
# Run all unit tests
make test-python-unit

# Run a specific test file
python -m pytest sdk/python/tests/unit/test_unit_feature_store.py -v

# Run a specific test by name
python -m pytest sdk/python/tests/unit/test_unit_feature_store.py -k "test_apply" -v

# Run fast unit tests only (no external dependencies)
make test-python-unit-fast
```

### Integration Tests (local)
```bash
# Run integration tests in local dev mode
make test-python-integration-local
```

## Linting and Formatting

```bash
# Format Python code
make format-python

# Lint Python code
make lint-python

# Run all precommit checks (format + lint)
make precommit-check

# Type checking (entire codebase)
uv run bash -c "cd sdk/python && mypy feast"

# Type-check a single file
uv run bash -c "cd sdk/python && mypy feast/path/to/file.py"
```

## Code Style

- Use type hints on all function signatures
- Use `from __future__ import annotations` at the top of new files
- Follow existing patterns in the module you are modifying
- PR titles must follow semantic conventions: `feat:`, `fix:`, `ci:`, `chore:`, `docs:`
- Add a GitHub label to PRs (e.g. `kind/bug`, `kind/feature`, `kind/housekeeping`)
- Sign off commits with `git commit -s` (DCO requirement)

## Docker Images

```bash
# Build all Docker images
make build-docker

# Build feature server Docker image
make build-feature-server-docker
```

## Documentation

```bash
# Build Sphinx documentation
make build-sphinx

# Build templates
make build-templates

# Build Helm docs
make build-helm-docs
```

## Documentation and Blog Posts

- **Blog posts must be placed in `/infra/website/docs/blog/`** — do NOT place them under `docs/blog/` or elsewhere.
- Blog post files must include YAML frontmatter with `title`, `description`, `date`, and `authors` fields matching the format of existing posts in that directory.
- All other docs go under `docs/` and **must be added to `docs/SUMMARY.md`** (GitBook navigation) or they won't appear on the website.

| Change type | Doc location |
|---|---|
| New online store | `docs/reference/online-stores/<name>.md` + update `README.md` and `SUMMARY.md` |
| New offline store | `docs/reference/offline-stores/<name>.md` + update `README.md`, `overview.md`, `SUMMARY.md` |
| New registry backend | `docs/reference/registries/<name>.md` + update `SUMMARY.md` |
| Config option | `docs/reference/feature-store-yaml.md` |
| CLI flag/command | `docs/reference/feast-cli-commands.md` |
| How-to / integration | `docs/how-to-guides/customizing-feast/` or `docs/how-to-guides/` + update `SUMMARY.md` |
| Architecture / concept | `docs/getting-started/architecture/` or `docs/getting-started/components/` |

## Project Structure

```
sdk/python/feast/          # Main Python SDK
  cli/cli.py               # CLI entry point (feast apply, feast materialize, etc.)
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
