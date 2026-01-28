# dbt Integration Tests

Integration tests for Feast's dbt integration feature, which allows importing dbt models as Feast FeatureViews.

## Overview

These tests verify the complete workflow of:
1. Parsing dbt `manifest.json` files using dbt-artifacts-parser
2. Extracting model metadata (columns, types, tags, descriptions)
3. Creating Feast objects (Entity, DataSource, FeatureView) from dbt models
4. Generating Python code with Feast definitions

## Test Coverage

### TestDbtManifestParsing
- Parse manifest metadata (dbt version, project name)
- Extract all models from manifest
- Filter models by dbt tags
- Filter models by name
- Parse model properties (database, schema, table, description, tags)
- Parse column metadata (name, type, description)

### TestDbtToFeastMapping
- Create BigQuery data sources from dbt models
- Create Snowflake data sources from dbt models
- Create File data sources from dbt models
- Create Feast entities
- Create FeatureViews with proper schema
- Exclude entity and timestamp columns from features
- Handle custom excluded columns
- Create all objects together (Entity + DataSource + FeatureView)

### TestDbtDataSourceTypes
- Test all supported data source types (bigquery, snowflake, file)
- Verify unsupported types raise errors

### TestDbtCodeGeneration
- Generate Python code from dbt models
- Generate code for different data source types
- Verify generated code structure and imports

### TestDbtTypeMapping
- Map dbt types to Feast types correctly:
  - STRING → String
  - INT32 → Int32
  - INT64 → Int64
  - FLOAT32 → Float32
  - FLOAT64 → Float64
  - TIMESTAMP → UnixTimestamp

### TestDbtIntegrationWorkflow
- End-to-end workflow with multiple models
- Code generation workflow with file output

## Test Data

Tests use a pre-generated dbt manifest from `test_dbt_project/`:
- 3 models: driver_features, customer_features, product_features
- Various column types and tags for comprehensive testing
- No external dependencies (database, dbt CLI) required

## Running Tests

Run all dbt integration tests:
```bash
pytest sdk/python/tests/integration/dbt/ -v
```

Run specific test class:
```bash
pytest sdk/python/tests/integration/dbt/test_dbt_integration.py::TestDbtManifestParsing -v
```

Run specific test:
```bash
pytest sdk/python/tests/integration/dbt/test_dbt_integration.py::TestDbtManifestParsing::test_parse_manifest_metadata -v
```

## Dependencies

Required:
- `dbt-artifacts-parser>=0.6.0` - For parsing dbt manifest files
- `feast` - Core Feast SDK with all data source types

## CI/CD

Tests run in GitHub Actions:
- `.github/workflows/dbt-integration-tests.yml` - Dedicated dbt test workflow
- `.github/workflows/unit_tests.yml` - As part of general unit tests

## Related Code

- `feast/dbt/parser.py` - dbt manifest parser
- `feast/dbt/mapper.py` - dbt to Feast object mapper
- `feast/dbt/codegen.py` - Python code generator
- `feast/cli/dbt_import.py` - CLI commands for dbt import
