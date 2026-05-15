# Feast - Agent Instructions

This file provides instructions for AI coding agents (GitHub Copilot, Claude Code, OpenAI Codex, etc.) working in this repository.

## Project Overview

Feast is an open source feature store for machine learning that helps ML platform teams manage features consistently for training and serving.

Feast (Feature Store) is a Python-based project that provides:
- **Offline Store**: Process historical data for batch scoring or model training
- **Online Store**: Power real-time predictions with low-latency features
- **Feature Server**: Serve pre-computed features online
- **Point-in-time correctness**: Prevent data leakage during model training
- **Data infrastructure abstraction**: Decouple ML from data infrastructure

## Development Commands

### Setup
```bash
make install-python-dependencies-dev
make install-python-dependencies-minimal
```

### Code Quality
```bash
# Format Python code (entire codebase)
make format-python

# Lint Python code (entire codebase)
make lint-python

# Full type check (entire codebase)
uv run bash -c "cd sdk/python && mypy feast"

# Full type check including tests
make mypy-full
```

#### Single-file lint and type-check
When working on a specific file, run checks scoped to that file to get fast feedback:
```bash
uv run ruff check sdk/python/feast/path/to/file.py          # lint
uv run ruff check --fix sdk/python/feast/path/to/file.py    # lint + auto-fix
uv run ruff format sdk/python/feast/path/to/file.py         # format
uv run bash -c "cd sdk/python && mypy feast/path/to/file.py" # type-check
```

### Testing
```bash
make test-python-unit                  # unit tests
make test-python-integration-local    # integration tests (local)
make test-python-integration          # integration tests (CI)
make test-python-universal            # all Python tests
```

### Protobuf Compilation
```bash
make compile-protos-python   # Python protobufs
make protos                  # all protos
```

### Go Development
```bash
make build-go && make test-go && make format-go && make lint-go
```

## Key Technologies

- **Languages**: Python (primary), Go
- **Dependencies**: pandas, pyarrow, SQLAlchemy, FastAPI, protobuf
- **Data Sources**: BigQuery, Snowflake, Redshift, Parquet, Postgres, Spark
- **Online Stores**: Redis, DynamoDB, Bigtable, Snowflake, SQLite, Postgres
- **Offline Stores**: BigQuery, Snowflake, Redshift, Spark, Dask, DuckDB
- **Cloud Providers**: AWS, GCP, Azure

## Agent Skills

The `skills/` directory contains tool-agnostic skills (compatible with Claude Code, OpenAI Codex, and other agent tools):

| Skill | Path | Use when… |
|---|---|---|
| **feast-user-guide** | `skills/feast-user-guide/SKILL.md` | Working with Feast as a user: defining features, retrieval, CLI, RAG |
| **feast-dev** | `skills/feast-dev/SKILL.md` | Contributing to Feast: setup, tests, Docker, docs, PR workflow |
| **feast-architecture** | `skills/feast-architecture/SKILL.md` | Understanding how each component works: registry, materialization, feature server, data flows |
| **feast-testing** | `skills/feast-testing/SKILL.md` | Writing tests, running targeted tests, debugging registry/online store issues |

Reference docs: `skills/references/` — feature definitions, configuration, retrieval & RAG.

Architecture & design intent: `docs/getting-started/architecture/` (overview, write patterns, RBAC), `docs/getting-started/components/` (per-component pages), `docs/adr/` (design decisions).

## Code Style

- Use type hints on all Python function signatures
- Follow existing patterns in the module you are modifying
- PR titles must follow semantic conventions: `feat:`, `fix:`, `ci:`, `chore:`, `docs:`
- Sign off commits with `git commit -s` (DCO requirement)
- Uses `ruff` for Python linting and formatting; Go uses standard `gofmt`
- Recompile protos after making changes to `.proto` files (`make protos`)
- Changes to core functionality may require updates across both Python and Go SDKs

## Documentation and Blog Posts

- **Blog posts must be placed in `/infra/website/docs/blog/`** — do NOT create blog posts under `docs/blog/` or any other location.
- Blog post files must include YAML frontmatter with `title`, `description`, `date`, and `authors` fields, following the format of existing posts in that directory.
- All other reference documentation goes under `docs/`.

## Contributing

1. Follow the [contribution guide](docs/project/contributing.md)
2. Set up your development environment
3. Run relevant tests before submitting PRs
4. Ensure code passes linting and type checking
