# Feast - Feature Store for Machine Learning

Feast is an open source feature store for machine learning that helps ML platform teams manage features consistently for training and serving.

## Project Overview

Feast (Feature Store) is a Python-based project that provides:
- **Offline Store**: Process historical data for batch scoring or model training
- **Online Store**: Power real-time predictions with low-latency features
- **Feature Server**: Serve pre-computed features online
- **Point-in-time correctness**: Prevent data leakage during model training
- **Data infrastructure abstraction**: Decouple ML from data infrastructure

## Development Commands

### Setup
```bash
# Install development dependencies
make install-python-dependencies-dev

# Install minimal dependencies
make install-python-dependencies-minimal
```

### Code Quality
```bash
# Format Python code
make format-python

# Lint Python code
make lint-python

# Type check
cd sdk/python && python -m mypy feast
```

### Testing
```bash
# Run unit tests
make test-python-unit

# Run integration tests (local)
make test-python-integration-local

# Run integration tests (CI)
make test-python-integration

# Run all Python tests
make test-python-universal
```

### Protobuf Compilation
```bash
# Compile Python protobuf files
make compile-protos-python

# Compile all protos
make protos
```

### Go Development
```bash
# Build Go code
make build-go

# Test Go code
make test-go

# Format Go code
make format-go

# Lint Go code
make lint-go
```

### Docker
```bash
# Build all Docker images
make build-docker

# Build feature server Docker image
make build-feature-server-docker
```

### Documentation
```bash
# Build Sphinx documentation
make build-sphinx

# Build templates
make build-templates

# Build Helm docs
make build-helm-docs
```

## Project Structure

```
feast/
├── sdk/python/          # Python SDK and core implementation
├── go/                 # Go implementation
├── ui/                 # Web UI
├── docs/               # Documentation
├── examples/           # Example projects
├── infra/              # Infrastructure and deployment
│   ├── charts/         # Helm charts
│   └── feast-operator/ # Kubernetes operator
└── protos/             # Protocol buffer definitions
```

## Key Technologies

- **Languages**: Python (primary), Go
- **Dependencies**: pandas, pyarrow, SQLAlchemy, FastAPI, protobuf
- **Data Sources**: BigQuery, Snowflake, Redshift, Parquet, Postgres, Spark
- **Online Stores**: Redis, DynamoDB, Bigtable, Snowflake, SQLite, Postgres
- **Offline Stores**: BigQuery, Snowflake, Redshift, Spark, Dask, DuckDB
- **Cloud Providers**: AWS, GCP, Azure

## Common Development Tasks

### Running Tests
The project uses pytest for Python testing with extensive integration test suites for different data sources and stores.

### Code Style
- Uses `ruff` for Python linting and formatting
- Go uses standard `gofmt`

### Protobuf Development
Protocol buffers are used for data serialization and gRPC APIs. Recompile protos after making changes to `.proto` files.

### Multi-language Support
Feast supports Python and Go SDKs. Changes to core functionality may require updates across both languages.

## Contributing

1. Follow the [contribution guide](docs/project/contributing.md)
2. Set up your development environment
3. Run relevant tests before submitting PRs
4. Ensure code passes linting and type checking